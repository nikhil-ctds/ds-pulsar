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
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.Transport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.opensearch.action.bulk.BulkProcessor;
import org.opensearch.client.json.ToJsonp;
import org.opensearch.client.opensearch._global.BulkRequest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class ElasticSearchBWCTests {

    public final static String INDEX = "myindex" + UUID.randomUUID().toString();

    static ElasticSearchConfig config;
    static ElasticSearchClient client;

    @BeforeClass
    public static final void initBeforeClass() throws IOException {
        config = new ElasticSearchConfig();
        config.setElasticSearchUrl("http://localhost:9200");
        // config.setElasticSearchUrl("http://localhost:9200");
        config.setIndexName(INDEX);
        client = new ElasticSearchClient(config);
    }



    @Test

    public void testIndexDelete() throws Exception {

        client.createIndexIfNeeded(INDEX);
        RestClient restClient = null;

        //Initialize the client with SSL and TLS enabled
        restClient = RestClient.builder(new HttpHost("localhost", 9200, "http")).build();
        ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        final ElasticsearchClient elasticsearchClient = new ElasticsearchClient(transport);

        final Map mapped = new HashMap();
        mapped.put("hello", "you");
        final co.elastic.clients.elasticsearch.core.BulkRequest bulkRequest1 = new co.elastic.clients.elasticsearch.core.BulkRequest.Builder()
                .operations(new BulkOperation
                        .Builder()
                        .index(
                                new IndexOperation.Builder<>()
                                        .index(INDEX).id("1").document(mapped).build()
                        )
                        .build())
                .build();

        final BulkResponse respon = elasticsearchClient.bulk(bulkRequest1);
        System.out.println(respon);

        final co.elastic.clients.elasticsearch.core.IndexRequest<Object> indexRequest = new co.elastic.clients.elasticsearch.core.IndexRequest.Builder<>().index(config.getIndexName())
                .document(mapped)
                .id("1")
                .build();
        final IndexResponse index = elasticsearchClient.index(indexRequest);

    }

}
