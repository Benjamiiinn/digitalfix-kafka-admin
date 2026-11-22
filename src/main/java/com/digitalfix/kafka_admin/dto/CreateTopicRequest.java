package com.digitalfix.kafka_admin.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

public record CreateTopicRequest(
    @NotBlank
        @Pattern(
                regexp = "^[a-zA-Z0-9._-]+$",
                message = "El nombre contiene caracteres no permitidos"
        )
        String name,

        @Min(1)
        @Max(100)
        int partitions,

        @Min(1)
        @Max(3)
        short replicationFactor
) {}
