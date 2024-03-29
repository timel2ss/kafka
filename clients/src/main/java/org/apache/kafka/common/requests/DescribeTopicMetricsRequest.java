package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.DescribeTopicMetricsRequestData;
import org.apache.kafka.common.message.DescribeTopicMetricsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;

public class DescribeTopicMetricsRequest extends AbstractRequest {
    private final DescribeTopicMetricsRequestData data;

    public static class Builder extends AbstractRequest.Builder<DescribeTopicMetricsRequest> {
        private final DescribeTopicMetricsRequestData data;

        public Builder(DescribeTopicMetricsRequestData data) {
            super(ApiKeys.DESCRIBE_TOPIC_METRICS);
            this.data = data;
        }

        @Override
        public DescribeTopicMetricsRequest build(short version) {
            return new DescribeTopicMetricsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    public DescribeTopicMetricsRequest(DescribeTopicMetricsRequestData data, short version) {
        super(ApiKeys.DESCRIBE_LOG_DIRS, version);
        this.data = data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new DescribeTopicMetricsResponse(
                new DescribeTopicMetricsResponseData()
                        .setThrottleTimeMs(throttleTimeMs)
                        .setErrorCode(Errors.forException(e).code()));
    }

    @Override
    public ApiMessage data() {
        return data;
    }

    public static DescribeTopicMetricsRequest parse(ByteBuffer buffer, short version) {
        return new DescribeTopicMetricsRequest(new DescribeTopicMetricsRequestData(
                new ByteBufferAccessor(buffer), version), version);
    }
}
