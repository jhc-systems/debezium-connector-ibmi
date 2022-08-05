package com.fnz.kafka.connector.health.beans;

import java.util.List;


public record ConnectorStatus (
    String name,
    Connector connector,
    List<Task> tasks,
    Source type
//    {"name":"msdevt-include","connector":{"state":"RUNNING","worker_id":"10.10.20.77:8083"},"tasks":[{"id":0,"state":"RUNNING","worker_id":"10.10.20.77:8083"}],"type":"source"}
) {}
