DROP TABLE IF EXISTS clientInstances;
DROP TABLE IF EXISTS clientConnectionStats;
DROP TABLE IF EXISTS clientProcedureStats;

CREATE TABLE clientInstances (
    instanceId                  INTEGER PRIMARY KEY AUTO_INCREMENT,
    clusterStartTime            TIMESTAMP NOT NULL,
    clusterLeaderAddress        int NOT NULL,
    applicationName             varchar(32) NOT NULL,
    subApplicationName          varchar(32),
    numHosts                    int NOT NULL,
    numSites                    int NOT NULL,
    numPartitions               int NOT NULL
);

CREATE TABLE clientConnectionStats (
    instanceId                  INTEGER NOT NULL REFERENCES clientInstances,
    tsEvent                     TIMESTAMP NOT NULL,
    hostname                    varchar(64) NOT NULL,
    connectionId                bigint NOT NULL,
    serverHostId                bigint NOT NULL,
    serverHostname              varchar(64) NOT NULL,
    serverConnectionId          bigint NOT NULL,
    numInvocations              bigint NOT NULL,
    numAborts                   bigint NOT NULL,
    numFailures                 bigint NOT NULL,
    numThrottled                bigint NOT NULL,
    numBytesRead                bigint NOT NULL,
    numMessagesRead             bigint NOT NULL,
    numBytesWritten             bigint NOT NULL,
    numMessagesWritten          bigint NOT NULL
);

CREATE TABLE clientProcedureStats (
    instanceId                  INTEGER NOT NULL REFERENCES clientInstances,
    tsEvent                     TIMESTAMP NOT NULL,
    hostname                    varchar(64) NOT NULL,
    connectionId                bigint NOT NULL,
    serverHostId                bigint NOT NULL,
    serverHostname              varchar(64) NOT NULL,
    serverConnectionId          bigint NOT NULL,
    procedureName               varchar(64) NOT NULL,
    roundtripAvg                int NOT NULL,
    roundtripMin                int NOT NULL,
    roundtripMax                int NOT NULL,
    clusterRoundtripAvg         bigint NOT NULL,
    clusterRoundtripMin         bigint NOT NULL,
    clusterRoundtripMax         bigint NOT NULL,
    numInvocations              bigint NOT NULL,
    numAborts                   bigint NOT NULL,
    numFailures                 bigint NOT NULL,
    numRestarts                 bigint NOT NULL
);
