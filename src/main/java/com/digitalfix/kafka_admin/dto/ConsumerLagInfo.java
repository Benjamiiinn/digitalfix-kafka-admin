package com.digitalfix.kafka_admin.dto;

import java.util.List;

public record ConsumerLagInfo(
        String groupId,
        long totalLag,
        List<PartitionLagInfo> partitions
) {
    public record PartitionLagInfo(
            String topic,
            int partition,
            long committedOffset,
            long endOffset,
            long lag
    ) {}
}
