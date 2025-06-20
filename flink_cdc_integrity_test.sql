-- Create parent table (CBA_CI)
CREATE TABLE CBA_CI (
  CI_ID STRING,
  PRIMARY KEY (CI_ID) NOT ENFORCED
) WITH (
  'topic' = 'flink_poc',
  'format' = 'json',
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


-- Flink SQL join to detect orphan records (Gives Issues Below)
-- (Something went wrong.
--  Table sink 'default.cluster_0.ORPHAN_CI_ADR' doesn't support consuming update and delete changes which is produced by node Join(joinType=[LeftOuterJoin], 
--   where=[(CI_ID = CI_ID0)], select=[CI_ID, CI_A1_1, CI_ID0], leftInputSpec=[JoinKeyContainsUniqueKey], rightInputSpec=[JoinKeyContainsUniqueKey]))
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

---Sink Table for Matched Records

CREATE TABLE MATCHED_CI_IDS (
  CI_ID STRING,
  CI_A1_1 STRING,
  CI_STATE_C STRING,
  SYS_STUS_C STRING,
  MATCH_STATUS STRING
) ;
---Sink Table for Orphaned Records (DLQ)

CREATE TABLE DLQ_CI_ADR (
  CI_ID STRING,
  CI_A1_1 STRING,
  CI_STATE_C STRING,
  SYS_STUS_C STRING,
  ERROR_REASON STRING
) ;


--Insert Into Matched Output (Inner Join) ( Gives Issues Below )
-- (Something went wrong.
--Table sink 'default.cluster_0.DLQ_CI_ADR' doesn't support consuming update and delete changes which is produced 
-- by node Join(joinType=[LeftOuterJoin], where=[(CI_ID = CI_ID0)], select=[CI_ID, CI_A1_1, CI_STATE_C, SYS_STUS_C, CI_ID0], leftInputSpec=[JoinKeyContainsUniqueKey], rightInputSpec=[JoinKeyContainsUniqueKey]))

INSERT INTO MATCHED_CI_IDS
SELECT 
  adr.CI_ID,
  adr.CI_A1_1,
  adr.CI_STATE_C,
  adr.SYS_STUS_C,
  'MATCHED' AS MATCH_STATUS
FROM 
  CBA_CI_ADR adr
JOIN 
  CBA_CI ci
ON 
  adr.CI_ID = ci.CI_ID;


---Insert Into DLQ (Left Join + Filter Nulls) (Gives Issues Below)
-- Table sink 'default.cluster_0.DLQ_CI_ADR' doesn't support consuming update and delete changes which is produced by node Join(joinType=[LeftOuterJoin], where=[(CI_ID = CI_ID0)], 
-- select=[CI_ID, CI_A1_1, CI_STATE_C, SYS_STUS_C, CI_ID0], leftInputSpec=[JoinKeyContainsUniqueKey], rightInputSpec=[JoinKeyContainsUniqueKey])

INSERT INTO DLQ_CI_ADR
SELECT 
  adr.CI_ID,
  adr.CI_A1_1,
  adr.CI_STATE_C,
  adr.SYS_STUS_C,
  'NO MATCH IN CBA_CI' AS ERROR_REASON
FROM 
  CBA_CI_ADR adr
LEFT JOIN 
  CBA_CI ci
ON 
  adr.CI_ID = ci.CI_ID
WHERE 
  ci.CI_ID IS NULL;
