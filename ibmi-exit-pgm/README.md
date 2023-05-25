# Debezium Connector for Db2 for i - Exit Program

This is a utility for the [Db2 for i Dezebium Connector](https://github.com/jhc-systems/debezium-connector-ibmi) project.

The connector reads the IBM i journal and relays this information to Kafka. If a journal receiver were to be removed before the connector has fully processed, however, journal entries would be missed and data loss would occur.

IBM i has a facility to prevent journal receivers being deleted. We can register a program as an [Exit Program](https://www.ibm.com/docs/en/i/7.5?topic=concepts-exit-programs) and the system will call it to confirm whether a deletion command for a journal receiver may proceed.

## Deployment

The exit program and associated database must deployed into the same library. The exit program will only ever check the database tables in the same library that contains it.

Beyond that restrictions, you may deploy it multiple times on your system if you wish. Just register an exit program for each instance, and they will ensure that journal receivers are not deleted prematurely.

Follow one of the two options below to either build the objects from source, or deploy from a save file.

### Building from source

You must have `git` installed on your system. If you have not done so, please follow the instructions at http://ibm.biz/ibmi-rpms

Open a shell (SSH preferred, but QShell should also work). Change to a suitable working directory and clone this repository to your IBM i:

    cd $HOME
    /QOpenSys/pkgs/bin/git clone https://github.com/jhc-systems/debezium-connector-ibmi-exit-pgm
    cd debezium-connector-ibmi-exit-pgm

There are two shell scripts to build the project. As an argument to each, pass the name of the library where you would like the objects to be deployed:

    ./builddb.sh DEBEZIUM
    ./buildpgm.sh DEBEZIUM

NOTE: If the database library already exists, perform a back up prior to building, as the build process may cause data loss of registered connectors and processed journal receivers.

### From save file

TODO

## Authorities

For security, review the authorities of the DEBEXIT program to ensure that unauthorised users cannot replace the program with custom code.

Review the authorities for the database objects created and ensure these are sufficient to allow the user(s) running the Debezium connector to be able to write data to the tables.

## Registration

With the database and exit program deployed in the library of your choice, you must register the exit program with the system.

Enter the following command, replacing `mylib` in the `PGM` argument as appropriate:

    ADDEXITPGM EXITPNT(QIBM_QJO_DLT_JRNRCV) FORMAT(DRCV0100) PGMNBR(*LOW) PGM(mylib/DEBEXIT)