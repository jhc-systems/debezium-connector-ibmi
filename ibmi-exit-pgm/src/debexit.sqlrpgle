**free

ctl-opt copyright('Copyright JHC Systems 2023')
        option(*nodebugio: *srcstmt)
        thread(*concurrent)
        actgrp('DEBEZIUM')
        main(debexit);

exec sql
  SET OPTION commit = *none;

/include ./debexit.rpgleinc


dcl-ds psds qualified psds;
  programLibrary char(10) pos(81);
end-ds;


dcl-proc debexit;
  dcl-pi *n;
    exitInfo likeds(t_DRCV0100) const; 
    statusInfo likeds(t_dltjrnrcvStatus);
  end-pi;

  statusInfo.length = %size(t_dltjrnrcvStatus);
  statusInfo.deleteStatus = PREVENT_DELETION;

  if isReceiverRequired(
    psds.programLibrary :
    exitInfo.journalReceiver :
    exitInfo.journalReceiverLibrary :
    exitInfo.journal :
    exitInfo.journalLibrary
  );
    return;
  endif;

  statusInfo.deleteStatus = PERMIT_DELETION;
end-proc;


dcl-proc isReceiverRequired;
  dcl-pi *n ind;
    registryLibrary varchar(10) const;
    receiverName char(10) value;
    receiverLibrary char(10) value;
    journalName char(10) value;
    journalLibrary char(10) value;
  end-pi;

  dcl-s statement varchar(1000);
  dcl-s preparedLibrary char(10) static;
  dcl-s found char(1);
  dcl-s nullInd int(5);

  if preparedLibrary <> registryLibrary;
    statement = 'VALUES (  -
                  SELECT ''Y''  -
                  FROM ' + %trimr(registryLibrary) + '/debezium_connector AS c  -
                  WHERE c.journal_name = ?  -
                    AND c.journal_library = ?  -
                    AND c.deleted IS NULL  -
                    AND c.connector_id NOT IN (  -
                      SELECT connector_id  -
                      FROM ' + %trimr(registryLibrary) + '/debezium_processed_receiver AS p  -
                      WHERE p.receiver_name = ?  -
                        AND p.receiver_library = ?  -
                  )  -
                  LIMIT 1  -
                ) INTO ?';

    exec sql
      PREPARE unprocessed_receivers FROM :statement;

    preparedLibrary = registryLibrary;
  endif;

  exec sql
    EXECUTE unprocessed_receivers
    USING
      :journalName,
      :journalLibrary,
      :receiverName,
      :receiverLibrary,
      :found :nullInd;
  
  return (SQLSTATE = '00000' and nullInd = 0);
end-proc;