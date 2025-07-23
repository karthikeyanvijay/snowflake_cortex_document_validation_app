-- Switch to the application role
USE ROLE FROSTLOGIC_ROLE;

-- Create database and schema
USE DATABASE FROSTLOGIC_DB;
CREATE SCHEMA IF NOT EXISTS PUBLIC;
USE SCHEMA PUBLIC;

USE WAREHOUSE FROSTLOGIC_WH; 

CREATE OR REPLACE HYBRID TABLE FILE_TYPE_CONFIG (
    FILE_TYPE STRING NOT NULL,
    FILE_DESCRIPTION STRING,
    CHUNK_SIZE INTEGER,
    CHUNK_OVERLAP INTEGER,
    TARGET_LAG STRING,
    
    -- Primary Key for hybrid table
    PRIMARY KEY (FILE_TYPE)
);

-- Insert default configurations
INSERT INTO FILE_TYPE_CONFIG (
    FILE_TYPE,
    FILE_DESCRIPTION,
    CHUNK_SIZE,
    CHUNK_OVERLAP,
    TARGET_LAG
) VALUES 
(
    'MSA',
    'Master Service Agreement documents',
    1000,
    100,
    '1 minute'
),
(
    'SOW',
    'Statement of Work documents',
    1200,
    120,
    '1 minute'
);