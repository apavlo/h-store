CREATE TABLE clientInstances (
    instanceId                  int NOT NULL AUTO_INCREMENT, -- SQLITE: AUTOINCREMENT
    clusterStartTime            bigint NOT NULL,
    clusterLeaderAddress        varchar(64) NOT NULL,
    applicationName             varchar(32) NOT NULL,
    subApplicationName          varchar(32),
    PRIMARY KEY (instanceId)
);

CREATE TABLE clientConnectionStats (


);

CREATE TABLE clientProcedureStats (

);