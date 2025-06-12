CREATE TABLE SINK_ORPHAN_CI_ADR (
  CI_ID STRING,
  CI_A1_1 STRING,
  ERROR_REASON STRING,
  PRIMARY KEY (CI_ID) NOT ENFORCED
) WITH (
  'connector' = 'confluent',
  'kafka.topic' = 'sink_orphan_ci_adr',
  'format' = 'avro-registry'
);

INSERT INTO SINK_ORPHAN_CI_ADR
SELECT
  adr.CI_ID,
  adr.CI_A1_1,
  'NO MATCH FOUND' AS ERROR_REASON
FROM SOURCE_CBA_CI_ADR adr
LEFT JOIN SOURCE_CBA_CI ci
  ON adr.CI_ID = ci.CI_ID
WHERE ci.CI_ID IS NULL;
