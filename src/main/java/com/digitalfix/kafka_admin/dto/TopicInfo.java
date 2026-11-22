package com.digitalfix.kafka_admin.dto;

import java.util.List;

public record TopicInfo(
        String name,
        int partitions,
        List<PartitionInfo> partitionDetails
) {
    public record PartitionInfo(
            int partition,
            int leader,
            List<Integer> replicas,
            List<Integer> inSyncReplicas
    ) {}
}
