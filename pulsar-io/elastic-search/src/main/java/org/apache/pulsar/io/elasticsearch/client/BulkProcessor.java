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
public abstract class BulkProcessor implements Closeable {

    @Builder
    @Getter
    public static class BulkOperationRequest {
        private long operationId;
    }

    @Builder
    @Getter
    public static class BulkOperationResult {
        private String error;
        private String index;
        private String documentId;
        public boolean isError() {
            return error != null;
        }
    }

    public interface Listener {

        void afterBulk(long executionId, List<BulkOperationRequest> bulkOperationList, List<BulkOperationResult> results);

        void afterBulk(long executionId, List<BulkOperationRequest> bulkOperationList, Throwable throwable);
    }

    @Builder
    @Getter
    public static class BulkIndexRequest {
        private long requestId;
        private String index;
        private String documentId;
        private String documentSource;
    }

    @Builder
    @Getter
    public static class BulkDeleteRequest {
        private long requestId;
        private String index;
        private String documentId;
    }


    public abstract void appendIndexRequest(BulkIndexRequest request) throws IOException;

    public abstract void appendDeleteRequest(BulkDeleteRequest request) throws IOException;

    public abstract void flush();

    public abstract void awaitClose(long timeout, TimeUnit unit) throws InterruptedException;

}
