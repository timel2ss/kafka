package org.apache.kafka.common.requests;


import org.apache.kafka.common.message.DescribeTopicMetricsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class DescribeTopicMetricsResponse extends AbstractResponse {

    private final DescribeTopicMetricsResponseData data;

    public DescribeTopicMetricsResponse(DescribeTopicMetricsResponseData data) {
        super(ApiKeys.DESCRIBE_TOPIC_METRICS);
        this.data = data;
    }

    @Override
    public DescribeTopicMetricsResponseData data() {
        return data;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        errorCounts.put(Errors.forCode(data.errorCode()), 1);
        return errorCounts;
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        data.setThrottleTimeMs(throttleTimeMs);
    }

    public static DescribeTopicMetricsResponse parse(ByteBuffer buffer, short version) {
        return new DescribeTopicMetricsResponse(new DescribeTopicMetricsResponseData(
                new ByteBufferAccessor(buffer), version));
    }
}
