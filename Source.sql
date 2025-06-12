CREATE TABLE SOURCE_CBA_CI (
  CI_ID STRING,
  CI_STATE_C STRING,
  PRIMARY KEY (CI_ID) NOT ENFORCED
) WITH (
  'connector' = 'confluent',
  'kafka.topic' = 'source_cba_ci',
  'format' = 'avro-registry'
);

CREATE TABLE SOURCE_CBA_CI_ADR (
  CI_ID STRING,
  CI_A1_1 STRING,
  PRIMARY KEY (CI_ID) NOT ENFORCED
) WITH (
  'connector' = 'confluent',
  'kafka.topic' = 'source_cba_ci_adr',
  'format' = 'avro-registry'
);
