package org.apache.kafka.clients.admin;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@InterfaceStability.Evolving
public class DescribeTopicMetricsResult {
    private final Map<Integer, KafkaFuture<Map<String, Long>>> futures;

    DescribeTopicMetricsResult(Map<Integer, KafkaFuture<Map<String, Long>>> futures) {
        this.futures = futures;
    }


    /**
     * Return a map from brokerId to future which can be used to check the information of partitions on each individual broker.
     * The result of the future is a map from broker log directory path to a description of that log directory.
     */
    public Map<Integer, KafkaFuture<Map<String, Long>>> topicMetrics() {
        return futures;
    }

    /**
     * Return a future which succeeds only if all the brokers have responded without error.
     * The result of the future is a map from brokerId to a map from broker log directory path
     * to a description of that log directory.
     */
    public KafkaFuture<Map<Integer, Map<String, Long>>> allTopicMetrics() {
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0])).
                thenApply(v -> {
                    Map<Integer, Map<String, Long>> topicMetrics = new HashMap<>(futures.size());
                    for (Map.Entry<Integer, KafkaFuture<Map<String, Long>>> entry : futures.entrySet()) {
                        try {
                            topicMetrics.put(entry.getKey(), entry.getValue().get());
                        } catch (InterruptedException | ExecutionException e) {
                            // This should be unreachable, because allOf ensured that all the futures completed successfully.
                            throw new RuntimeException(e);
                        }
                    }
                    return topicMetrics;
                });
    }
}
