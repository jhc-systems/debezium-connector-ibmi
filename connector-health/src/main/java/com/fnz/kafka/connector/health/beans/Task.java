package com.fnz.kafka.connector.health.beans;

import com.fasterxml.jackson.annotation.JsonProperty;

public record Task (
    String id,
    State state,
    @JsonProperty("worker_id") String workerId
//    {"id":0,"state":"RUNNING","worker_id":"10.10.20.77:8083"}
) { }
