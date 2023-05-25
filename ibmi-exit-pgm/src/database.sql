CREATE OR REPLACE TABLE debezium_connector
FOR SYSTEM NAME debconnect 
(
    CONSTRAINT pk_debezium_connector
    PRIMARY KEY (connector_id),
    
    connector_id FOR COLUMN id INT NOT NULL,
    connector_name FOR COLUMN connname VARCHAR(100) NOT NULL,
    journal_name FOR COLUMN jrnnam CHAR(10) NOT NULL,
    journal_library FOR COLUMN jrnlib CHAR(10) NOT NULL,
    deleted TIMESTAMP
);


DROP INDEX IF EXISTS debezium_connector_01;

CREATE INDEX debezium_connector_01
FOR SYSTEM NAME debconnec1
ON debezium_connector (journal_library, journal_name);


CREATE OR REPLACE TABLE debezium_processed_receiver
FOR SYSTEM NAME debprcrcv
(
    CONSTRAINT pk_debezium_processed_receiver
    PRIMARY KEY (connector_id, receiver_name, receiver_library),
        
    connector_id FOR COLUMN id INT NOT NULL,
    receiver_name FOR COLUMN jrnrcvnam CHAR(10) NOT NULL,
    receiver_library FOR COLUMN jrnrcvlib CHAR(10) NOT NULL,
    completed TIMESTAMP NOT NULL
        DEFAULT CURRENT TIMESTAMP,
    
    CONSTRAINT fk_debezium_connector_debezium_processed_receiver
    FOREIGN KEY (connector_id) REFERENCES debezium_connector (connector_id)
    ON UPDATE RESTRICT
    ON DELETE CASCADE
);


DROP INDEX IF EXISTS debezium_processed_receiver_01;

CREATE INDEX debezium_processed_receiver_01
FOR SYSTEM NAME debprcrcv1
ON debezium_processed_receiver (receiver_library, receiver_name);