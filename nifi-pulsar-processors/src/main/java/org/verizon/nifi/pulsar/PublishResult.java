package org.verizon.nifi.pulsar;

import org.apache.nifi.flowfile.FlowFile;

import java.util.Collection;
import java.util.Collections;

public interface PublishResult {

    boolean isFailure();

    int getSuccessfulMessageCount(FlowFile flowFile);

    Exception getReasonForFailure(FlowFile flowFile);

    public static PublishResult EMPTY = new PublishResult() {
        @Override
        public boolean isFailure() {
            return false;
        }

        @Override
        public int getSuccessfulMessageCount(FlowFile flowFile) {
            return 0;
        }

        @Override
        public Exception getReasonForFailure(FlowFile flowFile) {
            return null;
        }
    };
}
