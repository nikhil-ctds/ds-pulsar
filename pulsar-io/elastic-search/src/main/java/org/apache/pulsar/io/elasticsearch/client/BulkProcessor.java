package org.apache.pulsar.io.elasticsearch.client;

import lombok.Builder;
import lombok.Getter;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Processor for "bulk" call to the Elastic REST Endpoint.
 */
public interface BulkProcessor extends Closeable {

    @Builder
    @Getter
    class BulkOperationRequest {
        private long operationId;
    }

    @Builder
    @Getter
    class BulkOperationResult {
        private String error;
        private String index;
        private String documentId;
        public boolean isError() {
            return error != null;
        }
    }

    interface Listener {

        void afterBulk(long executionId, List<BulkOperationRequest> bulkOperationList, List<BulkOperationResult> results);

        void afterBulk(long executionId, List<BulkOperationRequest> bulkOperationList, Throwable throwable);
    }

    @Builder
    @Getter
    class BulkIndexRequest {
        private long requestId;
        private String index;
        private String documentId;
        private String documentSource;
    }

    @Builder
    @Getter
    class BulkDeleteRequest {
        private long requestId;
        private String index;
        private String documentId;
    }


    void appendIndexRequest(BulkIndexRequest request) throws IOException;

    void appendDeleteRequest(BulkDeleteRequest request) throws IOException;

    void flush();

    void awaitClose(long timeout, TimeUnit unit) throws InterruptedException;

}
