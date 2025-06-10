-- Create parent table (CBA_CI)
CREATE TABLE CBA_CI (
  CI_ID STRING,
  PRIMARY KEY (CI_ID) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'odh7ci_CBA_CI',
  'properties.bootstrap.servers' = '<your-bootstrap-server>',
  'format' = 'json',
  'scan.startup.mode' = 'latest-offset'
);

-- Create child table (CBA_CI_ADR)
CREATE TABLE CBA_CI_ADR (
  CI_ID STRING,
  CI_A1_1 STRING,
  CI_A1_2 STRING,
  CI_STATE_C STRING,
  SYS_STUS_C STRING,
  UPDT_SEQ_N INT,
  PRIMARY KEY (CI_ID) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'odh7adr_CBA_CI_ADR',
  'properties.bootstrap.servers' = '<your-bootstrap-server>',
  'format' = 'json',
  'scan.startup.mode' = 'latest-offset'
);

-- Create sink table for orphan records
CREATE TABLE ORPHAN_CI_ADR (
  CI_ID STRING,
  CI_A1_1 STRING,
  reason STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'orphan_ci_adr_records',
  'properties.bootstrap.servers' = '<your-bootstrap-server>',
  'format' = 'json'
);

-- Flink SQL join to detect orphan records
INSERT INTO ORPHAN_CI_ADR
SELECT
  adr.CI_ID,
  adr.CI_A1_1,
  'No matching CI_ID in CBA_CI' AS reason
FROM
  CBA_CI_ADR adr
LEFT JOIN
  CBA_CI ci
ON
  adr.CI_ID = ci.CI_ID
WHERE
  ci.CI_ID IS NULL;
