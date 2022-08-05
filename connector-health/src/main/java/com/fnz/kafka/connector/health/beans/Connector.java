package com.fnz.kafka.connector.health.beans;

import com.fasterxml.jackson.annotation.JsonProperty;

public record Connector (
    State state,
    @JsonProperty("worker_id") String workerId
//    "connector":{"state":"RUNNING","worker_id":"10.10.20.77:8083"}
) { }
