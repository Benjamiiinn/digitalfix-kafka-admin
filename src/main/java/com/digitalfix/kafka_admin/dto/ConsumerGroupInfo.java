package com.digitalfix.kafka_admin.dto;

public record ConsumerGroupInfo(
        String groupId,
        String state
) {}
