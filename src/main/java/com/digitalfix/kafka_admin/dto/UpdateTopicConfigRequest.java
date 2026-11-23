package com.digitalfix.kafka_admin.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Pattern;

public record UpdateTopicConfigRequest(
        @Min(1000)
        Long retentionMs,

        @Pattern(
                regexp = "delete|compact|compact,delete",
                message = "cleanupPolicy debe ser delete, compact o compact,delete"
        )
        String cleanupPolicy
) {}
