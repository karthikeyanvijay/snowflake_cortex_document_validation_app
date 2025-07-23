-- ========================================
-- FROSTLOGIC MANAGEMENT PROCEDURES
-- ========================================

USE ROLE FROSTLOGIC_ROLE;
USE DATABASE FROSTLOGIC_DB;
USE SCHEMA PUBLIC;
USE WAREHOUSE FROSTLOGIC_WH;

-- ========================================
-- CREATE PROCESSING CONFIG HYBRID TABLE
-- ========================================

CREATE OR REPLACE HYBRID TABLE FILE_PROCESSING_CONFIG (
    CONFIG_NAME STRING NOT NULL,
    PROCESSING_TYPE STRING NOT NULL, -- 'CORTEX_SEARCH' or 'AI_EXTRACT'
    CONFIG_MODEL STRING DEFAULT 'claude-4-sonnet',
    CONFIG_JSON STRING NOT NULL,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (CONFIG_NAME)
);


-- ========================================
-- FILE TYPE CONFIG MANAGEMENT PROCEDURES
-- ========================================

CREATE OR REPLACE PROCEDURE FILE_TYPE_CONFIGS_GET()
RETURNS TABLE(FILE_TYPE STRING, FILE_DESCRIPTION STRING, CHUNK_SIZE INTEGER, CHUNK_OVERLAP INTEGER, TARGET_LAG STRING)
LANGUAGE SQL
AS
$$
BEGIN
    LET result RESULTSET := (
        SELECT 
            FILE_TYPE,
            FILE_DESCRIPTION,
            CHUNK_SIZE,
            CHUNK_OVERLAP,
            TARGET_LAG
        FROM FILE_TYPE_CONFIG
        ORDER BY FILE_TYPE
    );
    RETURN TABLE(result);
END;
$$;

CREATE OR REPLACE PROCEDURE FILE_TYPE_CONFIGS_CREATE(
    FILE_TYPE STRING,
    FILE_DESCRIPTION STRING,
    CHUNK_SIZE INTEGER DEFAULT 1000,
    CHUNK_OVERLAP INTEGER DEFAULT 100,
    TARGET_LAG STRING DEFAULT '1 minute'
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
BEGIN
    -- Validate that file type is not reserved name
    IF (UPPER(:FILE_TYPE) = 'STREAMLIT') THEN
        RETURN OBJECT_CONSTRUCT(
            'success', FALSE,
            'error', 'File type STREAMLIT is reserved and cannot be used'
        );
    END IF;
    
    -- Check if file type already exists
    LET check_count INTEGER := (
        SELECT COUNT(*) FROM FILE_TYPE_CONFIG WHERE FILE_TYPE = :FILE_TYPE
    );
    
    IF (check_count > 0) THEN
        RETURN OBJECT_CONSTRUCT(
            'success', FALSE,
            'error', 'File type ' || :FILE_TYPE || ' already exists'
        );
    END IF;
    
    -- Insert new file type configuration
    INSERT INTO FILE_TYPE_CONFIG (
        FILE_TYPE, FILE_DESCRIPTION, CHUNK_SIZE, CHUNK_OVERLAP, TARGET_LAG
    ) VALUES (
        :FILE_TYPE, :FILE_DESCRIPTION, :CHUNK_SIZE, :CHUNK_OVERLAP, :TARGET_LAG
    );
    
    RETURN OBJECT_CONSTRUCT(
        'success', TRUE,
        'message', 'File type configuration created successfully',
        'file_type', :FILE_TYPE
    );
END;
$$;

CREATE OR REPLACE PROCEDURE FILE_TYPE_CONFIGS_UPDATE(
    FILE_TYPE STRING,
    FILE_DESCRIPTION STRING,
    CHUNK_SIZE INTEGER,
    CHUNK_OVERLAP INTEGER,
    TARGET_LAG STRING
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
BEGIN
    -- Check if file type exists
    LET check_count INTEGER := (
        SELECT COUNT(*) FROM FILE_TYPE_CONFIG WHERE FILE_TYPE = :FILE_TYPE
    );
    
    IF (check_count = 0) THEN
        RETURN OBJECT_CONSTRUCT(
            'success', FALSE,
            'error', 'File type ' || :FILE_TYPE || ' not found'
        );
    END IF;
    
    -- Update file type configuration
    UPDATE FILE_TYPE_CONFIG 
    SET 
        FILE_DESCRIPTION = :FILE_DESCRIPTION,
        CHUNK_SIZE = :CHUNK_SIZE,
        CHUNK_OVERLAP = :CHUNK_OVERLAP,
        TARGET_LAG = :TARGET_LAG
    WHERE FILE_TYPE = :FILE_TYPE;
    
    RETURN OBJECT_CONSTRUCT(
        'success', TRUE,
        'message', 'File type configuration updated successfully',
        'file_type', :FILE_TYPE
    );
END;
$$;

CREATE OR REPLACE PROCEDURE FILE_TYPE_CONFIGS_DELETE(
    FILE_TYPE STRING,
    DROP_DATA BOOLEAN DEFAULT FALSE
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'delete_file_type_handler'
AS
$$
import json
from snowflake.snowpark import Session

def delete_file_type_handler(session: Session, file_type: str, drop_data: bool):
    try:
        # Check if file type exists
        check_sql = f"SELECT COUNT(*) as count FROM FILE_TYPE_CONFIG WHERE FILE_TYPE = '{file_type}'"
        check_result = session.sql(check_sql).collect()
        
        if not check_result or check_result[0]["COUNT"] == 0:
            return {
                "success": False,
                "error": f"File type {file_type} not found"
            }
        
        result = {"success": True, "actions": []}
        
        # Call cleanup pipeline procedure
        try:
            cleanup_sql = f"CALL CLEANUP_PIPELINE('{file_type}', {str(drop_data).lower()})"
            cleanup_result = session.sql(cleanup_sql).collect()
            
            if cleanup_result:
                cleanup_data = cleanup_result[0]["CLEANUP_PIPELINE"]
                if isinstance(cleanup_data, str):
                    cleanup_data = json.loads(cleanup_data)
                
                if cleanup_data.get("success"):
                    result["actions"].extend(cleanup_data.get("actions", []))
                else:
                    result["actions"].append(f"Pipeline cleanup warning: {cleanup_data.get('error', 'Unknown error')}")
        except Exception as e:
            result["actions"].append(f"Pipeline cleanup failed: {str(e)}")
        
        # Delete from file type config
        delete_sql = f"DELETE FROM FILE_TYPE_CONFIG WHERE FILE_TYPE = '{file_type}'"
        session.sql(delete_sql).collect()
        result["actions"].append(f"File type configuration for {file_type} deleted")
        
        result["message"] = f"File type {file_type} deleted successfully"
        
        return result
        
    except Exception as e:
        return {"success": False, "error": f"Delete operation failed: {str(e)}"}
$$;

-- ========================================
-- PROCESSING CONFIG MANAGEMENT PROCEDURES
-- ========================================

CREATE OR REPLACE PROCEDURE PROCESSING_CONFIGS_GET()
RETURNS TABLE(CONFIG_NAME STRING, PROCESSING_TYPE STRING, CONFIG_MODEL STRING, CONFIG_JSON STRING, CREATED_AT TIMESTAMP_NTZ, UPDATED_AT TIMESTAMP_NTZ)
LANGUAGE SQL
AS
$$
BEGIN
    LET result RESULTSET := (
        SELECT 
            CONFIG_NAME,
            PROCESSING_TYPE,
            CONFIG_MODEL,
            CONFIG_JSON,
            CREATED_AT,
            UPDATED_AT
        FROM FILE_PROCESSING_CONFIG
        ORDER BY CONFIG_NAME
    );
    RETURN TABLE(result);
END;
$$;

CREATE OR REPLACE PROCEDURE PROCESSING_CONFIGS_CREATE(
    CONFIG_NAME STRING,
    PROCESSING_TYPE STRING,
    CONFIG_JSON STRING,
    CONFIG_MODEL STRING DEFAULT 'claude-4-sonnet'
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
BEGIN
    -- Validate processing type
    IF (:PROCESSING_TYPE NOT IN ('CORTEX_SEARCH', 'AI_EXTRACT')) THEN
        RETURN OBJECT_CONSTRUCT(
            'success', FALSE,
            'error', 'Invalid processing type. Must be CORTEX_SEARCH or AI_EXTRACT'
        );
    END IF;
    
    -- Check if config name already exists
    LET check_count INTEGER := (
        SELECT COUNT(*) FROM FILE_PROCESSING_CONFIG WHERE CONFIG_NAME = :CONFIG_NAME
    );
    
    IF (check_count > 0) THEN
        RETURN OBJECT_CONSTRUCT(
            'success', FALSE,
            'error', 'Processing config ' || :CONFIG_NAME || ' already exists'
        );
    END IF;
    
    -- Insert new processing configuration
    INSERT INTO FILE_PROCESSING_CONFIG (
        CONFIG_NAME, PROCESSING_TYPE, CONFIG_JSON, CONFIG_MODEL, UPDATED_AT
    ) VALUES (
        :CONFIG_NAME, :PROCESSING_TYPE, :CONFIG_JSON, :CONFIG_MODEL, CURRENT_TIMESTAMP()
    );
    
    RETURN OBJECT_CONSTRUCT(
        'success', TRUE,
        'message', 'Processing configuration created successfully',
        'config_name', :CONFIG_NAME
    );
END;
$$;

CREATE OR REPLACE PROCEDURE PROCESSING_CONFIGS_UPDATE(
    CONFIG_NAME STRING,
    PROCESSING_TYPE STRING,
    CONFIG_JSON STRING,
    CONFIG_MODEL STRING
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
BEGIN
    -- Validate processing type
    IF (:PROCESSING_TYPE NOT IN ('CORTEX_SEARCH', 'AI_EXTRACT')) THEN
        RETURN OBJECT_CONSTRUCT(
            'success', FALSE,
            'error', 'Invalid processing type. Must be CORTEX_SEARCH or AI_EXTRACT'
        );
    END IF;
    
    -- Check if config exists
    LET check_count INTEGER := (
        SELECT COUNT(*) FROM FILE_PROCESSING_CONFIG WHERE CONFIG_NAME = :CONFIG_NAME
    );
    
    IF (check_count = 0) THEN
        RETURN OBJECT_CONSTRUCT(
            'success', FALSE,
            'error', 'Processing config ' || :CONFIG_NAME || ' not found'
        );
    END IF;
    
    -- Update processing configuration
    UPDATE FILE_PROCESSING_CONFIG 
    SET 
        PROCESSING_TYPE = :PROCESSING_TYPE,
        CONFIG_JSON = :CONFIG_JSON,
        CONFIG_MODEL = :CONFIG_MODEL,
        UPDATED_AT = CURRENT_TIMESTAMP()
    WHERE CONFIG_NAME = :CONFIG_NAME;
    
    RETURN OBJECT_CONSTRUCT(
        'success', TRUE,
        'message', 'Processing configuration updated successfully',
        'config_name', :CONFIG_NAME
    );
END;
$$;

CREATE OR REPLACE PROCEDURE PROCESSING_CONFIGS_DELETE(
    CONFIG_NAME STRING
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
BEGIN
    -- Check if config exists
    LET check_count INTEGER := (
        SELECT COUNT(*) FROM FILE_PROCESSING_CONFIG WHERE CONFIG_NAME = :CONFIG_NAME
    );
    
    IF (check_count = 0) THEN
        RETURN OBJECT_CONSTRUCT(
            'success', FALSE,
            'error', 'Processing config ' || :CONFIG_NAME || ' not found'
        );
    END IF;
    
    -- Delete processing configuration
    DELETE FROM FILE_PROCESSING_CONFIG WHERE CONFIG_NAME = :CONFIG_NAME;
    
    RETURN OBJECT_CONSTRUCT(
        'success', TRUE,
        'message', 'Processing configuration deleted successfully',
        'config_name', :CONFIG_NAME
    );
END;
$$;

CREATE OR REPLACE PROCEDURE PROCESSING_CONFIGS_VALIDATE(
    CONFIG_JSON STRING
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'validate_config_handler'
AS
$$
import json
from snowflake.snowpark import Session

def validate_config_handler(session: Session, config_json):
    try:
        # Parse JSON if string
        if isinstance(config_json, str):
            config = json.loads(config_json)
        else:
            config = config_json
        
        errors = []
        warnings = []
        
        # Check required top-level fields
        if "extraction_config" not in config:
            errors.append("Missing required field: extraction_config")
        
        if "evaluation_config" not in config:
            warnings.append("Missing evaluation_config - evaluation will not be performed")
        
        if "search_limit" not in config:
            warnings.append("Missing search_limit - will use default value")
        elif not isinstance(config["search_limit"], int) or config["search_limit"] < 1 or config["search_limit"] > 10:
            errors.append("search_limit must be an integer between 1 and 10")
        
        # Validate extraction_config
        if "extraction_config" in config:
            extraction = config["extraction_config"]
            if not isinstance(extraction, dict):
                errors.append("extraction_config must be an object")
            elif len(extraction) == 0:
                errors.append("extraction_config cannot be empty")
            else:
                for key, value in extraction.items():
                    if not isinstance(value, str) or not value.strip():
                        errors.append(f"extraction_config.{key} must be a non-empty string")
        
        # Validate evaluation_config if present
        if "evaluation_config" in config:
            evaluation = config["evaluation_config"]
            if not isinstance(evaluation, dict):
                errors.append("evaluation_config must be an object")
            else:
                for key, value in evaluation.items():
                    if not isinstance(value, str) or not value.strip():
                        errors.append(f"evaluation_config.{key} must be a non-empty string")
        
        # Check for matching keys between extraction and evaluation
        if ("extraction_config" in config and "evaluation_config" in config and 
            isinstance(config["extraction_config"], dict) and isinstance(config["evaluation_config"], dict)):
            
            extraction_keys = set(config["extraction_config"].keys())
            evaluation_keys = set(config["evaluation_config"].keys())
            
            if extraction_keys != evaluation_keys:
                warnings.append("extraction_config and evaluation_config have different field names")
        
        return {
            "is_valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings,
            "field_count": len(config.get("extraction_config", {}))
        }
        
    except json.JSONDecodeError as e:
        return {
            "is_valid": False,
            "errors": [f"Invalid JSON format: {str(e)}"],
            "warnings": []
        }
    except Exception as e:
        return {
            "is_valid": False,
            "errors": [f"Validation error: {str(e)}"],
            "warnings": []
        }
$$;

-- ========================================
-- FILE MANAGEMENT PROCEDURES
-- ========================================

CREATE OR REPLACE PROCEDURE FILES_GET_BY_TYPE(
    FILE_TYPE STRING
)
RETURNS TABLE(FILE_NAME STRING, FULL_PATH STRING, STAGE_NAME STRING, CHUNK_COUNT INTEGER, FIRST_PROCESSED TIMESTAMP_NTZ, LAST_PROCESSED TIMESTAMP_NTZ)
LANGUAGE SQL
AS
$$
BEGIN
    LET chunks_table STRING := :FILE_TYPE || '_CHUNKS';
    LET stage_name STRING := :FILE_TYPE || '_STAGE';
    
    LET result RESULTSET := (
        EXECUTE IMMEDIATE 'SELECT DISTINCT 
            FILE_NAME,
            FULL_PATH,
            STAGE_NAME,
            COUNT(*) as CHUNK_COUNT,
            MIN(CREATED_AT) as FIRST_PROCESSED,
            MAX(CREATED_AT) as LAST_PROCESSED
        FROM ' || chunks_table || '
        GROUP BY FILE_NAME, FULL_PATH, STAGE_NAME
        ORDER BY LAST_PROCESSED DESC'
    );
    RETURN TABLE(result);
END;
$$;

CREATE OR REPLACE PROCEDURE AVAILABLE_FILE_TYPES_GET()
RETURNS TABLE(FILE_TYPE STRING)
LANGUAGE SQL
AS
$$
BEGIN
    LET result RESULTSET := (
        SELECT DISTINCT FILE_TYPE
        FROM FILE_TYPE_CONFIG
        ORDER BY FILE_TYPE
    );
    RETURN TABLE(result);
END;
$$;

-- ========================================
-- INSERT DEFAULT PROCESSING CONFIGURATIONS
-- ========================================

-- Clear any existing default configurations
DELETE FROM FILE_PROCESSING_CONFIG WHERE CONFIG_NAME IN ('config1', 'config2');

-- Insert CORTEX_SEARCH configuration
INSERT INTO FILE_PROCESSING_CONFIG (CONFIG_NAME, PROCESSING_TYPE, CONFIG_MODEL, CONFIG_JSON) VALUES 
('config1', 'CORTEX_SEARCH', 'claude-4-sonnet', '{"extraction_config": {"effective_date": "What is the effective date or start date of this agreement? Look for specific dates when the contract becomes effective.", "agreement_duration": "What is the duration or term of this agreement? Look for how long the contract lasts (e.g., 1 year, 2 years, etc.).", "notice_period": "What is the notice period required for termination? Look for how much advance notice is needed to terminate the agreement.", "payment_terms": "What are the payment terms? Look for information about when and how payments should be made (e.g., net 30, monthly, etc.).", "standard_rate_per_hour": "What is the standard hourly rate or rate per hour mentioned in this agreement? Look for specific dollar amounts per hour.", "force_majeure_clause": "Find any force majeure clauses. Extract the specific text that discusses force majeure, acts of God, or circumstances beyond control.", "indemnification_clause": "Find any indemnification clauses. Extract specific text about indemnification, liability protection, or holding harmless provisions.", "renewal_options_clause": "Find any renewal or extension clauses. Extract text about contract renewal options, automatic renewals, or extension terms.", "confidentiality_clause": "Find any confidentiality or non-disclosure clauses. Extract specific text about confidential information, NDAs, or privacy requirements.", "data_security_clause": "Find any data security or data protection clauses. Extract text about data security requirements, data protection measures, or security standards."}, "evaluation_config": {"effective_date": "Does the SOW start date fall within the MSA effective period and comply with master agreement timing requirements?", "agreement_duration": "Is the SOW duration within the MSA term limits and does it comply with maximum allowable project durations?", "notice_period": "Are the SOW termination notice periods consistent with MSA requirements and governance procedures?", "payment_terms": "Do the SOW payment terms fully comply with MSA payment requirements and approved financial procedures?", "standard_rate_per_hour": "Are the SOW hourly rates within the MSA approved rate structure and do they comply with pricing guidelines?", "force_majeure_clause": "Do the SOW force majeure provisions align with MSA requirements for scope, coverage, and procedural compliance?", "indemnification_clause": "Are the SOW indemnification terms consistent with MSA requirements regarding liability allocation and protection scope?", "renewal_options_clause": "Do the SOW renewal terms comply with MSA requirements regarding extension periods, conditions, and approval processes?", "confidentiality_clause": "Are the SOW confidentiality requirements consistent with MSA standards for information protection and disclosure restrictions?", "data_security_clause": "Do the SOW data security standards meet or exceed the MSA requirements for data protection and privacy compliance?"}, "search_limit": 3}');

-- Insert AI_EXTRACT configuration
INSERT INTO FILE_PROCESSING_CONFIG (CONFIG_NAME, PROCESSING_TYPE, CONFIG_MODEL, CONFIG_JSON) VALUES 
('config2', 'AI_EXTRACT', 'claude-4-sonnet', '{"extraction_config": {"effective_date": "What is the effective date or start date of this agreement? Look for specific dates when the contract becomes effective.", "agreement_duration": "What is the duration or term of this agreement? Look for how long the contract lasts (e.g., 1 year, 2 years, etc.).", "notice_period": "What is the notice period required for termination? Look for how much advance notice is needed to terminate the agreement.", "payment_terms": "What are the payment terms? Look for information about when and how payments should be made (e.g., net 30, monthly, etc.).", "standard_rate_per_hour": "What is the standard hourly rate or rate per hour mentioned in this agreement? Look for specific dollar amounts per hour.", "force_majeure_clause": "Find any force majeure clauses. Extract the specific text that discusses force majeure, acts of God, or circumstances beyond control.", "indemnification_clause": "Find any indemnification clauses. Extract specific text about indemnification, liability protection, or holding harmless provisions.", "renewal_options_clause": "Find any renewal or extension clauses. Extract text about contract renewal options, automatic renewals, or extension terms.", "confidentiality_clause": "Find any confidentiality or non-disclosure clauses. Extract specific text about confidential information, NDAs, or privacy requirements.", "data_security_clause": "Find any data security or data protection clauses. Extract text about data security requirements, data protection measures, or security standards."}, "evaluation_config": {"effective_date": "Does the SOW start date fall within the MSA effective period and comply with master agreement timing requirements?", "agreement_duration": "Is the SOW duration within the MSA term limits and does it comply with maximum allowable project durations?", "notice_period": "Are the SOW termination notice periods consistent with MSA requirements and governance procedures?", "payment_terms": "Do the SOW payment terms fully comply with MSA payment requirements and approved financial procedures?", "standard_rate_per_hour": "Are the SOW hourly rates within the MSA approved rate structure and do they comply with pricing guidelines?", "force_majeure_clause": "Do the SOW force majeure provisions align with MSA requirements for scope, coverage, and procedural compliance?", "indemnification_clause": "Are the SOW indemnification terms consistent with MSA requirements regarding liability allocation and protection scope?", "renewal_options_clause": "Do the SOW renewal terms comply with MSA requirements regarding extension periods, conditions, and approval processes?", "confidentiality_clause": "Are the SOW confidentiality requirements consistent with MSA standards for information protection and disclosure restrictions?", "data_security_clause": "Do the SOW data security standards meet or exceed the MSA requirements for data protection and privacy compliance?"}, "search_limit": 5}');
