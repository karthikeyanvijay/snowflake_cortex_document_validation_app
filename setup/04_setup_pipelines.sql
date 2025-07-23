-- Frostlogic Backend Comprehensive Testing Script
USE ROLE FROSTLOGIC_ROLE;
USE DATABASE FROSTLOGIC_DB;
USE SCHEMA PUBLIC;
USE WAREHOUSE FROSTLOGIC_WH;

-- ========================================
-- SETUP PIPELINES
-- ========================================

SELECT '=== SETTING UP MSA PIPELINE ===' as INFO;

CALL SETUP_FILE_PROCESSING_PIPELINE(
    FILE_TYPE => 'MSA',
    CHUNK_SIZE => 1000,
    CHUNK_OVERLAP => 100,
    TARGET_LAG => '1 minute'
);

SELECT '=== SETTING UP SOW PIPELINE ===' as INFO;

CALL SETUP_FILE_PROCESSING_PIPELINE(
    FILE_TYPE => 'SOW',
    CHUNK_SIZE => 1200,
    CHUNK_OVERLAP => 120,
    TARGET_LAG => '1 minute'
);
