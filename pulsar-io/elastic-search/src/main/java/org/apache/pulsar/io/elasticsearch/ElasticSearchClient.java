/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.io.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ErrorCause;
import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.DeleteRequest;
import co.elastic.clients.elasticsearch.core.DeleteResponse;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperationBuilders;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.bulk.DeleteOperation;
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.indices.DeleteIndexResponse;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.indices.IndexSettings;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.conn.NHttpClientConnectionManager;
import org.apache.http.nio.conn.NoopIOSessionStrategy;
import org.apache.http.nio.conn.SchemeIOSessionStrategy;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.functions.api.Record;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class ElasticSearchClient implements AutoCloseable {

    static final String[] malformedErrors = {
            "mapper_parsing_exception",
            "action_request_validation_exception",
            "illegal_argument_exception"
    };

    private ElasticSearchConfig config;
    private ConfigCallback configCallback;
    private ElasticsearchClient javaClient;

    final Set<String> indexCache = new HashSet<>();
    final Map<String, String> topicToIndexCache = new HashMap<>();

    final RandomExponentialRetry backoffRetry;
    final BulkProcessor bulkProcessor;
    final ConcurrentMap<Long, Record> records = new ConcurrentHashMap<>();
    final AtomicReference<Exception> irrecoverableError = new AtomicReference<>();
    final ScheduledExecutorService executorService;
    final AtomicLong bulkOperationIdGenerator = new AtomicLong();
    final ObjectMapper objectMapper = new ObjectMapper();

    ElasticSearchClient(ElasticSearchConfig elasticSearchConfig) throws MalformedURLException {
        this.config = elasticSearchConfig;
        this.configCallback = new ConfigCallback();
        this.backoffRetry = new RandomExponentialRetry(elasticSearchConfig.getMaxRetryTimeInSec());
        // idle+expired connection evictor thread
        this.executorService = Executors.newSingleThreadScheduledExecutor();
        this.executorService.scheduleAtFixedRate(new Runnable() {
                                                     @Override
                                                     public void run() {
                                                         configCallback.connectionManager.closeExpiredConnections();
                                                         configCallback.connectionManager.closeIdleConnections(
                                                                 config.getConnectionIdleTimeoutInMs(), TimeUnit.MILLISECONDS);
                                                     }
                                                 },
                config.getConnectionIdleTimeoutInMs(),
                config.getConnectionIdleTimeoutInMs(),
                TimeUnit.MILLISECONDS
        );

        URL url = new URL(config.getElasticSearchUrl());
        log.info("ElasticSearch URL {}", url);
        RestClientBuilder builder = RestClient.builder(new HttpHost(url.getHost(), url.getPort(), url.getProtocol()))
                .setRequestConfigCallback(new org.elasticsearch.client.RestClientBuilder.RequestConfigCallback() {
                    @Override
                    public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder builder) {
                        return builder
                                .setContentCompressionEnabled(config.isCompressionEnabled())
                                .setConnectionRequestTimeout(config.getConnectionRequestTimeoutInMs())
                                .setConnectTimeout(config.getConnectTimeoutInMs())
                                .setSocketTimeout(config.getSocketTimeoutInMs());
                    }
                })
                .setHttpClientConfigCallback(this.configCallback)
                .setFailureListener(new RestClient.FailureListener() {
                    public void onFailure(Node node) {
                        log.warn("Node host={} failed", node.getHost());
                    }
                });
        ElasticsearchTransport transport = new RestClientTransport(builder.build(),
                new JacksonJsonpMapper());
        this.javaClient = new ElasticsearchClient(transport);

        if (config.isBulkEnabled() == false) {
            bulkProcessor = null;
        } else {
            bulkProcessor = new BulkProcessor(elasticSearchConfig, javaClient, new BulkProcessor.Listener() {

                private Record removeAndGetRecordForOperation(BulkOperation operation) {
                    final BulkProcessor.BulkOperationWithId bulkOperation =
                            (BulkProcessor.BulkOperationWithId) operation;
                    return records.remove(bulkOperation.getOperationId());

                }
                @Override
                public void beforeBulk(long executionId, BulkRequest bulkRequest) {
                }

                @Override
                public void afterBulk(long executionId, BulkRequest bulkRequest, BulkResponse bulkResponse) {
                    log.trace("Bulk request id={} size={}:", executionId, bulkRequest.operations().size());
                    int index = 0;
                    for (BulkResponseItem item: bulkResponse.items()) {
                        final Record record = removeAndGetRecordForOperation(bulkRequest.operations().get(index++));
                        if (item.error() != null) {
                            record.fail();
                            checkForIrrecoverableError(item);
                        } else {
                            record.ack();
                        }
                    }
                }

                @Override
                public void afterBulk(long executionId, BulkRequest bulkRequest, Throwable throwable) {
                    log.warn("Bulk request id={} failed:", executionId, throwable);
                    for (BulkOperation operation: bulkRequest.operations()) {
                        final Record record = removeAndGetRecordForOperation(operation);
                        record.fail();
                    }
                }
            });

        }

    }

    void failed(Exception e) {
        if (irrecoverableError.compareAndSet(null, e)) {
            log.error("Irrecoverable error:", e);
        }
    }

    boolean isFailed() {
        return irrecoverableError.get() != null;
    }

    void checkForIrrecoverableError(BulkResponseItem bulkItemResponse) {
        final ErrorCause errorCause = bulkItemResponse.error();
        if (errorCause == null) {
            return;
        }
        for (String error : malformedErrors) {
            if (errorCause.type().contains(error)) {
                switch (config.getMalformedDocAction()) {
                    case IGNORE:
                        break;
                    case WARN:
                        log.warn("Ignoring malformed document index={} id={}",
                                bulkItemResponse.index(),
                                bulkItemResponse.id(),
                                errorCause.type());
                        break;
                    case FAIL:
                        log.error("Failure due to the malformed document index={} id={}",
                                bulkItemResponse.index(),
                                bulkItemResponse.id(),
                                errorCause.type());
                        failed(new Exception(errorCause.type()));
                        break;
                }
            }
        }
    }

    public void bulkIndex(Record record, Pair<String, String> idAndDoc) throws Exception {
        try {
            checkNotFailed();
            checkIndexExists(record.getTopicName());
            final String documentId = idAndDoc.getLeft();
            final String documentSource = idAndDoc.getRight();

            final Map mapped = objectMapper.readValue(documentSource, Map.class);

            final IndexOperation<Map> indexOperation = new IndexOperation.Builder<Map>()
                    .index(config.getIndexName())
                    .id(documentId)
                    .document(mapped)
                    .build();

            final long operationId = bulkOperationIdGenerator.incrementAndGet();
            records.put(operationId, record);

            long sourceLength = 0;
            if (config.getBulkSizeInMb() > 0) {
                sourceLength = documentSource.getBytes(StandardCharsets.UTF_8).length;
            }
            bulkProcessor.add(BulkProcessor.BulkOperationWithId.indexOperation(indexOperation,
                    operationId, sourceLength));
        } catch(Exception e) {
            log.debug("index failed id=" + idAndDoc.getLeft(), e);
            record.fail();
            throw e;
        }
    }

    public boolean indexDocumentWithRetry(Record<GenericObject> record, Pair<String, String> idAndDoc) {
        return retry(() -> indexDocument(record, idAndDoc), "index document");
    }

    /**
     * Index an elasticsearch document and ack the record.
     * @param record
     * @param idAndDoc
     * @return
     * @throws Exception
     */
    @SneakyThrows
    public boolean indexDocument(Record<GenericObject> record, Pair<String, String> idAndDoc) throws Exception {
        try {
            checkNotFailed();
            checkIndexExists(record.getTopicName());

            final Map mapped = objectMapper.readValue(idAndDoc.getRight(), Map.class);
            final IndexRequest<Object> indexRequest = new IndexRequest.Builder<>()
                    .index(config.getIndexName())
                    .document(mapped)
                    .id(idAndDoc.getLeft())
                    .build();
            final IndexResponse index = javaClient.index(indexRequest);

            if (index.result().equals(Result.Created) || index.result().equals(Result.Updated)) {
                record.ack();
                return true;
            } else {
                record.fail();
                return false;
            }
        } catch (final Exception ex) {
            log.warn("index failed id=" + idAndDoc.getLeft(), ex);
            record.fail();
            throw ex;
        }
    }

    public void bulkDelete(Record<GenericObject> record, String id) throws Exception {
        try {
            checkNotFailed();
            checkIndexExists(record.getTopicName());

            final DeleteOperation deleteOperation = new DeleteOperation.Builder()
                    .index(config.getIndexName())
                    .id(id)
                    .build();

            final long operationId = bulkOperationIdGenerator.incrementAndGet();
            records.put(operationId, record);
            bulkProcessor.add(BulkProcessor.BulkOperationWithId.deleteOperation(deleteOperation, operationId));
        } catch(Exception e) {
            log.debug("delete failed id=" + id, e);
            record.fail();
            throw e;
        }
    }

    public boolean deleteDocumentWithRetry(Record<GenericObject> record, String id) {
        return retry(() -> deleteDocument(record, id), "delete document");
    }

    /**
     * Delete an elasticsearch document and ack the record.
     * @param record
     * @param id
     * @return
     * @throws IOException
     */
    public boolean deleteDocument(Record<GenericObject> record, String id) throws Exception {
        try {
            checkNotFailed();
            checkIndexExists(record.getTopicName());


            final DeleteRequest req = new
                    DeleteRequest.Builder()
                    .index(config.getIndexName())
                    .id(id)
                    .build();

            DeleteResponse index = javaClient.delete(req);
            if (index.result().equals(Result.Deleted) || index.result().equals(Result.NotFound)) {
                record.ack();
                return true;
            } else {
                record.fail();
                return false;
            }
/*

            log.debug("delete result=" + deleteResponse.getResult());
            if (deleteResponse.getResult().equals(DocWriteResponse.Result.DELETED) ||
                    deleteResponse.getResult().equals(DocWriteResponse.Result.NOT_FOUND)) {
                record.ack();
                return true;
            }
            record.fail();
            return false;*/
        } catch (final Exception ex) {
            log.debug("index failed id=" + id, ex);
            record.fail();
            throw ex;
        }
    }

    /**
     * Flushes the bulk processor.
     */
    public void flush() {
        bulkProcessor.flush();
    }

    @Override
    public void close() {
        try {
            if (bulkProcessor != null) {
                bulkProcessor.awaitClose(5000L, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            log.warn("Elasticsearch bulk processor close error:", e);
        }
        this.executorService.shutdown();
        if (this.javaClient != null) {
            this.javaClient.shutdown();
        }
    }

    private void checkNotFailed() throws Exception {
        if (irrecoverableError.get() != null) {
            throw irrecoverableError.get();
        }
    }

    private void checkIndexExists(Optional<String> topicName) throws IOException {
        if (!config.isCreateIndexIfNeeded()) {
            return;
        }
        String indexName = indexName(topicName);
        if (!indexCache.contains(indexName)) {
            synchronized (this) {
                if (!indexCache.contains(indexName)) {
                    createIndexIfNeeded(indexName);
                    indexCache.add(indexName);
                }
            }
        }
    }

    private String indexName(Optional<String> topicName) throws IOException {
        if (config.getIndexName() != null) {
            // Use the configured indexName if provided.
            return config.getIndexName();
        }
        if (!topicName.isPresent()) {
            throw new IOException("Elasticsearch index name configuration and topic name are empty");
        }
        return topicToIndexName(topicName.get());
    }

    @VisibleForTesting
    public String topicToIndexName(String topicName) {
        return topicToIndexCache.computeIfAbsent(topicName, k -> {
            // see elasticsearch limitations https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html#indices-create-api-path-params
            String indexName = topicName.toLowerCase(Locale.ROOT);

            // remove the pulsar topic info persistent://tenant/namespace/topic
            String[] parts = indexName.split("/");
            if (parts.length > 1) {
                indexName = parts[parts.length-1];
            }

            // truncate to the max bytes length
            while (indexName.getBytes(StandardCharsets.UTF_8).length > 255) {
                indexName = indexName.substring(0, indexName.length() - 1);
            }
            if (indexName.length() <= 0 || !indexName.matches("[a-zA-Z\\.0-9][a-zA-Z_\\.\\-\\+0-9]*")) {
                throw new RuntimeException(new IOException("Cannot convert the topic name='" + topicName + "' to a valid elasticsearch index name"));
            }
            if (log.isDebugEnabled()) {
                log.debug("Translate topic={} to index={}", k, indexName);
            }
            return indexName;
        });
    }

    @VisibleForTesting
    public boolean createIndexIfNeeded(String indexName) throws IOException {
        if (indexExists(indexName)) {
            return false;
        }
        final co.elastic.clients.elasticsearch.indices.CreateIndexRequest createIndexRequest = new co.elastic.clients.elasticsearch.indices.CreateIndexRequest.Builder()
                .index(indexName)
                .settings(new IndexSettings.Builder()
                        .numberOfShards(config.getIndexNumberOfShards() + "")
                        .numberOfReplicas(config.getIndexNumberOfReplicas() + "")
                        .build()
                )
                .build();
        return retry(() -> {
            final co.elastic.clients.elasticsearch.indices.CreateIndexResponse createIndexResponse = javaClient.indices().create(createIndexRequest);
            if ((createIndexResponse.acknowledged() != null && createIndexResponse.acknowledged())|| createIndexResponse.shardsAcknowledged()) {
                return true;
            }
            throw new IOException("Unable to create index.");
        }, "create index");
    }

    public boolean indexExists(final String indexName) throws IOException {
        final ExistsRequest request = new ExistsRequest.Builder()
                .index(indexName)
                .build();
        return retry(() -> javaClient.indices().exists(request).value(), "index exists");
    }

    @VisibleForTesting
    protected long totalHits(String indexName) throws IOException {
        final co.elastic.clients.elasticsearch.core.SearchResponse<Map> searchResponse = search(indexName);

        for(Hit<Map> hit: searchResponse.hits().hits()) {
            System.out.println(hit.id() + ": " + hit.fields());
        }
        return searchResponse.hits().total().value();
    }

    @VisibleForTesting
    protected co.elastic.clients.elasticsearch.core.SearchResponse<Map> search(String indexName) throws IOException {
        final co.elastic.clients.elasticsearch.indices.RefreshRequest refreshRequest = new co.elastic.clients.elasticsearch.indices.RefreshRequest.Builder().index(indexName).build();
        javaClient.indices().refresh(refreshRequest);

        return javaClient.search(new co.elastic.clients.elasticsearch.core.SearchRequest.Builder().index(indexName)
                .q("*:*")
                .build(), Map.class);
    }

    @VisibleForTesting
    protected DeleteIndexResponse delete(String indexName) throws IOException {
        return javaClient.indices().delete(new co.elastic.clients.elasticsearch.indices.DeleteIndexRequest.Builder().index(indexName).build());
    }

    private <T> T retry(Callable<T> callable, String source) {
        try {
            return backoffRetry.retry(callable, config.getMaxRetries(), config.getRetryBackoffInMs(), source);
        } catch (Exception e) {
            e.printStackTrace();
            log.error("error in command {} wth retry", source, e);
            throw new ElasticSearchConnectionException(source + " failed", e);
        }
    }

    public class ConfigCallback implements RestClientBuilder.HttpClientConfigCallback {
        final NHttpClientConnectionManager connectionManager;
        final CredentialsProvider credentialsProvider;

        public ConfigCallback() {
            this.connectionManager = buildConnectionManager(ElasticSearchClient.this.config);
            this.credentialsProvider = buildCredentialsProvider(ElasticSearchClient.this.config);
        }

        @Override
        public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder builder) {
            builder.setMaxConnPerRoute(config.getBulkConcurrentRequests());
            builder.setMaxConnTotal(config.getBulkConcurrentRequests());
            builder.setConnectionManager(connectionManager);

            if (this.credentialsProvider != null) {
                builder.setDefaultCredentialsProvider(credentialsProvider);
            }
            return builder;
        }

        public NHttpClientConnectionManager buildConnectionManager(ElasticSearchConfig config) {
            try {
                IOReactorConfig ioReactorConfig = IOReactorConfig.custom()
                        .setConnectTimeout(config.getConnectTimeoutInMs())
                        .setSoTimeout(config.getSocketTimeoutInMs())
                        .build();
                ConnectingIOReactor ioReactor = new DefaultConnectingIOReactor(ioReactorConfig);
                PoolingNHttpClientConnectionManager connManager;
                if (config.getSsl().isEnabled()) {
                    ElasticSearchSslConfig sslConfig = config.getSsl();
                    HostnameVerifier hostnameVerifier = config.getSsl().isHostnameVerification()
                            ? SSLConnectionSocketFactory.getDefaultHostnameVerifier()
                            : new NoopHostnameVerifier();
                    String[] cipherSuites = null;
                    if (!Strings.isNullOrEmpty(sslConfig.getCipherSuites())) {
                        cipherSuites = sslConfig.getCipherSuites().split(",");
                    }
                    String[] protocols = null;
                    if (!Strings.isNullOrEmpty(sslConfig.getProtocols())) {
                        protocols = sslConfig.getProtocols().split(",");
                    }
                    Registry<SchemeIOSessionStrategy> registry = RegistryBuilder.<SchemeIOSessionStrategy>create()
                            .register("http", NoopIOSessionStrategy.INSTANCE)
                            .register("https", new SSLIOSessionStrategy(
                                    buildSslContext(config),
                                    protocols,
                                    cipherSuites,
                                    hostnameVerifier))
                            .build();
                    connManager = new PoolingNHttpClientConnectionManager(ioReactor, registry);
                } else {
                    connManager = new PoolingNHttpClientConnectionManager(ioReactor);
                }
                return connManager;
            } catch(Exception e) {
                throw new ElasticSearchConnectionException(e);
            }
        }

        private SSLContext buildSslContext(ElasticSearchConfig config) throws NoSuchAlgorithmException, KeyManagementException, CertificateException, KeyStoreException, IOException, UnrecoverableKeyException {
            ElasticSearchSslConfig sslConfig = config.getSsl();
            SSLContextBuilder sslContextBuilder = SSLContexts.custom();
            if (!Strings.isNullOrEmpty(sslConfig.getProvider())) {
                sslContextBuilder.setProvider(sslConfig.getProvider());
            }
            if (!Strings.isNullOrEmpty(sslConfig.getProtocols())) {
                sslContextBuilder.setProtocol(sslConfig.getProtocols());
            }
            if (!Strings.isNullOrEmpty(sslConfig.getTruststorePath()) && !Strings.isNullOrEmpty(sslConfig.getTruststorePassword())) {
                sslContextBuilder.loadTrustMaterial(new File(sslConfig.getTruststorePath()), sslConfig.getTruststorePassword().toCharArray());
            }
            if (!Strings.isNullOrEmpty(sslConfig.getKeystorePath()) && !Strings.isNullOrEmpty(sslConfig.getKeystorePassword())) {
                sslContextBuilder.loadKeyMaterial(new File(sslConfig.getKeystorePath()),
                        sslConfig.getKeystorePassword().toCharArray(),
                        sslConfig.getKeystorePassword().toCharArray());
            }
            return sslContextBuilder.build();
        }

        private CredentialsProvider buildCredentialsProvider(ElasticSearchConfig config) {
            if (StringUtils.isEmpty(config.getUsername()) || StringUtils.isEmpty(config.getPassword())) {
                return null;
            }
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(config.getUsername(), config.getPassword()));
            return credentialsProvider;
        }
    }
}
