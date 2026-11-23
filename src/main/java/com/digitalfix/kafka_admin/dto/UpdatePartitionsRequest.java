package com.digitalfix.kafka_admin.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

public record UpdatePartitionsRequest(
        @Min(1)
        @Max(100)
        int partitions
) {}
