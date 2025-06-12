# Flink CDC Integrity Test – Confluent Cloud

This project validates referential integrity between parent (`CBA_CI`) and child (`CBA_CI_ADR`) records using Flink SQL in Confluent Cloud.

## 📋 Steps

1. Go to **Flink > SQL Workspace** in Confluent Cloud.
2. Paste contents of `flink_cdc_integrity_test.sql` into the SQL editor.
3. Run each command one-by-one to:
   - Create parent and child Kafka-backed tables
   - Detect child records with missing parent references
4. Inject the sample JSON from `test_event_sample.json` into `odh7adr_CBA_CI_ADR`.
5. Check the output in `orphan_ci_adr_records` topic.

## 🧪 Test Case

This simulates a child record with `CI_ID` = `99999999` that does not exist in the parent table.
