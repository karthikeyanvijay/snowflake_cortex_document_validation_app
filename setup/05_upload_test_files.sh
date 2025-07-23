#! /bin/bash
snow stage copy ../sample_docs/msa/healthcareco_coredata_msa_v2024.pdf @FROSTLOGIC_DB.PUBLIC.MSA_STAGE;
snow stage copy ../sample_docs/sow/healthcareco_coredata_sow_v2024.pdf @FROSTLOGIC_DB.PUBLIC.SOW_STAGE;
snow stage copy ../sample_docs/msa/healthcareco_datapeak_msa_v2024.docx @FROSTLOGIC_DB.PUBLIC.MSA_STAGE;
snow stage copy ../sample_docs/sow/healthcareco_datapeak_SOW_v2024.docx @FROSTLOGIC_DB.PUBLIC.SOW_STAGE;
sleep 240