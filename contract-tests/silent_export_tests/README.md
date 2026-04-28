Silent export contract tests

Place contract tests here to validate:
- initial full-table export format and headers
- daily incremental append behaviour and marker update
- schema change handling (new file creation)

These tests should run against a test DB and the output directory `v1-backend/nifi_data/silent_exports/<tenant>/`.
