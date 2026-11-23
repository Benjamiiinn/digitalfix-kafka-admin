package com.digitalfix.kafka_admin.dto;

import java.util.Map;

public record TopicConfigInfo(
        String name,
        Map<String, String> configs
) {}
