package com.digitalfix.kafka_admin.dto;

public record BrokerInfo(
        int id,
        String host,
        int port
) {}
