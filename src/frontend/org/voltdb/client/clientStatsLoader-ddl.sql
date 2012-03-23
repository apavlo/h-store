CREATE TABLE clientInstances (
    instanceId                  int NOT NULL AUTO_INCREMENT, -- SQLITE: AUTOINCREMENT
    clusterStartTime            bigint NOT NULL,
    clusterLeaderAddress        varchar(64) NOT NULL,
    applicationName             varchar(32) NOT NULL,
    subApplicationName          varchar(32),
    PRIMARY KEY (instanceId)
);

CREATE TABLE clientConnectionStats (
    instanceId                  int NOT NULL AUTO_INCREMENT,
    tsEvent                     bigint NOT NULL,
    hostname                    varchar(64) NOT NULL,
    connectionId                bigint NOT NULL,
    serverHostId                bigint NOT NULL,
    serverHostname              varchar(64) NOT NULL,
    serverConnectionId          bigint NOT NULL,
    numInvocations              bigint NOT NULL,
    numAborts                   bigint NOT NULL,
    numFailures                 bigint NOT NULL,
    numBytesRead                bigint NOT NULL,
    numMessagesRead             bigint NOT NULL,
    numBytesWritten             bigint NOT NULL,
    numMessagesWritten          bigint NOT NULL,
    PRIMARY KEY (instanceId)
);

CREATE TABLE clientProcedureStats (
    instanceId                  int NOT NULL AUTO_INCREMENT,
    tsEvent                     bigint NOT NULL,
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
    PRIMARY KEY (instanceId)
);
