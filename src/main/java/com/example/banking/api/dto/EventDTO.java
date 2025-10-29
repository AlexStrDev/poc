package com.example.banking.api.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class EventDTO {
    private String eventIdentifier;
    private String aggregateIdentifier;
    private Long sequenceNumber;
    private String eventType;
    private Instant timestamp;
    private Map<String, Object> payload;
    private Map<String, Object> metaData;
}