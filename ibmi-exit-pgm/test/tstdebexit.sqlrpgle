**free

// A very rudimentary test. Relies on the database and exit program being deployed
// to library TSTDEBEXIT.

ctl-opt copyright('Copyright JHC Systems 2023')
        option(*nodebugio: *srcstmt)
        thread(*concurrent)
        actgrp(*new)
        main(main);

exec sql
  SET OPTION commit = *none;

/include ../src/debexit.rpgleinc


dcl-proc main;
  exec sql
    DELETE FROM tstdebexit.debezium_connector;
 
  exec sql
    INSERT INTO tstdebexit.debezium_connector
      (connector_id, connector_name, journal_name, journal_library, deleted)
    VALUES
      (1, 'Connector 1', 'JRN_A', 'JRNLIB', NULL),
      (2, 'Connector 2', 'JRN_B', 'JRNLIB', CURRENT TIMESTAMP),
      (3, 'Connector 3', 'JRN_C', 'JRNLIB', NULL),
      (4, 'Connector 4', 'JRN_C', 'JRNLIB', NULL);
    
  exec sql
    INSERT INTO tstdebexit.debezium_processed_receiver
      (connector_id, receiver_name, receiver_library)
    VALUES
      (3, 'RCV_C1', 'JRNLIB'),
      (3, 'RCV_C2', 'JRNLIB'),
      (4, 'RCV_C2', 'JRNLIB');


  snd-msg 'No connector monitoring the journal = permitted';
  testResult('JRN_Z' : 'JRNLIB' : 'RCV_Z1' : 'JRNLIB' : PERMIT_DELETION);

  snd-msg 'No record of the receiver for the monitored journal = denied';
  testResult('JRN_A' : 'JRNLIB' : 'RCV_A1' : 'JRNLIB' : PREVENT_DELETION);

  snd-msg 'A deleted connector does not prevent deletion';
  testResult('JRN_B' : 'JRNLIB' : 'RCV_B1' : 'JRNLIB' : PERMIT_DELETION);

  snd-msg 'A receiver must have been processed by all monitoring connectors';
  testResult('JRN_C' : 'JRNLIB' : 'RCV_C1' : 'JRNLIB' : PREVENT_DELETION);
  testResult('JRN_C' : 'JRNLIB' : 'RCV_C2' : 'JRNLIB' : PERMIT_DELETION);
end-proc;


dcl-proc testResult;
  dcl-pi *n;
    journalName varchar(10) const;
    journalLibrary varchar(10) const;
    receiverName varchar(10) const;
    receiverLibrary varchar(10) const;
    expectedResult varchar(1) const;
  end-pi;

  dcl-ds exitInfo likeds(t_DRCV0100);
  dcl-ds statusInfo likeds(t_dltjrnrcvStatus);

  exitInfo.journalReceiver = receiverName;
  exitInfo.journalReceiverLibrary = receiverLibrary;
  exitInfo.journal = journalName;
  exitInfo.journalLibrary = journalLibrary;

  debexit(exitInfo : statusInfo);

  if expectedResult <> statusInfo.deleteStatus;
    snd-msg *escape 'Failed on jrn ' + journalLibrary + '/' + journalName + ',' +
      ' rcv ' + receiverLibrary + '/' + receiverName;
  endif;
end-proc;