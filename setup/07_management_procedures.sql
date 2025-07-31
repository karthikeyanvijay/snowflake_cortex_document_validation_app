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


-- ========================================
-- FROSTLOGIC EMAIL NOTIFICATION PROCEDURE
-- ========================================
-- Sends HTML-formatted email notifications for document validation results

USE ROLE FROSTLOGIC_ROLE;
USE DATABASE FROSTLOGIC_DB;
USE SCHEMA PUBLIC;
USE WAREHOUSE FROSTLOGIC_WH;

CREATE OR REPLACE PROCEDURE SEND_VALIDATION_EMAIL(
    RESULTS_JSON VARIANT,
    EMAIL_ADDRESSES ARRAY,
    EMAIL_INTEGRATION STRING DEFAULT 'ACCOUNT_EMAIL_INTEGRATION',
    MODEL_NAME STRING DEFAULT 'claude-4-sonnet'
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main_handler'
EXECUTE AS CALLER
AS
$$
import json
import re
from datetime import datetime
from snowflake.snowpark import Session

def validate_inputs(session, results_json, email_addresses, email_integration, model_name):
    """
    Validates all input parameters and returns validation result.
    
    Returns:
        dict: {"valid": bool, "errors": list, "parsed_data": dict}
    """
    errors = []
    parsed_data = {}
    
    try:
        # Parse and validate JSON structure
        if isinstance(results_json, str):
            data = json.loads(results_json)
        else:
            data = results_json
        
        # Validate required JSON fields
        required_fields = ['results', 'files_analyzed', 'model_used', 'success']
        for field in required_fields:
            if field not in data:
                errors.append(f"Missing required field: {field}")
        
        if not data.get('success', False):
            errors.append("Results JSON indicates processing was not successful")
        
        if 'files_analyzed' in data and len(data['files_analyzed']) != 2:
            errors.append("Expected exactly 2 files in files_analyzed array")
        
        parsed_data['results_data'] = data
        
    except json.JSONDecodeError as e:
        errors.append(f"Invalid JSON format: {str(e)}")
    except Exception as e:
        errors.append(f"JSON parsing error: {str(e)}")
    
    # Validate email addresses
    if not email_addresses or len(email_addresses) == 0:
        errors.append("Email addresses array cannot be empty")
    else:
        valid_emails = []
        for email in email_addresses:
            if isinstance(email, str) and '@' in email and '.' in email:
                valid_emails.append(email.strip())
            else:
                errors.append(f"Invalid email format: {email}")
        parsed_data['valid_emails'] = valid_emails
    
    # Validate email integration name
    if not email_integration or not isinstance(email_integration, str):
        errors.append("Email integration name must be a valid string")
    
    # Validate model name
    if not model_name or not isinstance(model_name, str):
        errors.append("Model name must be a valid string")
    
    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "parsed_data": parsed_data
    }

def generate_ai_summary(session, results_data, model_name, threshold=0.75):
    """
    Generate compliant/non-compliant summary using AI_COMPLETE.
    
    Returns:
        dict: {"compliant_items": str, "non_compliant_items": str, "error": str}
    """
    try:
        # Prepare data for AI analysis with enhanced formatting rules
        analysis_prompt = f"""
Generate clean bullet point lists from document validation results.

CRITICAL FORMATTING REQUIREMENTS:
- Output ONLY HTML format using <li> and <strong> tags
- STRICTLY FORBIDDEN: asterisks (*), backticks (`), hash symbols (#), markdown syntax
- NO section headers - just bullet points
- Use HTML <strong> tags for emphasis, NOT markdown **bold**

Icon assignment by score:
- Scores >= {threshold}: Use ✅ (compliant)
- Scores 0.5 to {threshold}: Use ⚠️ (warning) 
- Scores < 0.5: Use ❌ (critical)

Required output format (one line per item):
<li>✅ <strong>Category Name:</strong> Brief explanation</li>
<li>❌ <strong>Category Name:</strong> Brief explanation</li>
<li>⚠️ <strong>Category Name:</strong> Brief explanation</li>

Document validation data:
{json.dumps(results_data.get('results', {}), indent=2)}

Return ONLY the <li> elements with appropriate icons based on scores. No headers, no markdown."""

        # Call AI_COMPLETE using Snowpark
        ai_response = session.sql(
            "SELECT SNOWFLAKE.CORTEX.COMPLETE(?, ?) as response",
            params=[model_name, analysis_prompt]
        ).collect()
        
        if ai_response and len(ai_response) > 0:
            ai_content = ai_response[0]['RESPONSE']
            
            # Parse AI response by separating lines with different icons
            lines = ai_content.split('\n')
            compliant_lines = []
            non_compliant_lines = []
            
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                    
                # Check for compliant items (✅)
                if '✅' in line:
                    compliant_lines.append(line)
                # Check for non-compliant items (❌ or ⚠️)
                elif '❌' in line or '⚠️' in line:
                    non_compliant_lines.append(line)
            
            # Format as HTML lists
            compliant_html = ""
            if compliant_lines:
                compliant_html = "<ul>" + "".join(f"{line}" if line.startswith('<li>') else f"<li>{line}</li>" for line in compliant_lines) + "</ul>"
            
            non_compliant_html = ""
            if non_compliant_lines:
                non_compliant_html = "<ul>" + "".join(f"{line}" if line.startswith('<li>') else f"<li>{line}</li>" for line in non_compliant_lines) + "</ul>"
            
            return {
                "compliant_items": compliant_html if compliant_html else "<p>No compliant items identified.</p>",
                "non_compliant_items": non_compliant_html if non_compliant_html else "<p>No non-compliant items identified.</p>",
                "error": None
            }
        else:
            raise Exception("No response from AI_COMPLETE")
            
    except Exception as e:
        # Fallback to programmatic summary with dynamic icons if AI fails
        compliant_items = []
        non_compliant_items = []
        
        for category, details in results_data.get('results', {}).items():
            evaluation = details.get('evaluation', {})
            score = evaluation.get('evaluation_score', 0)
            explanation = evaluation.get('evaluation_explanation', f'Score {score:.2f}')
            category_name = category.replace('_', ' ').title()
            
            # Dynamic icon selection based on score ranges
            if score >= threshold:
                icon = "✅"
                compliant_items.append(f"<li>{icon} <strong>{category_name}:</strong> {explanation}</li>")
            else:
                # Use tiered icons for non-compliant items
                if score < 0.5:
                    icon = "❌"  # Red X for severe issues
                else:
                    icon = "⚠️"  # Warning triangle for moderate issues
                non_compliant_items.append(f"<li>{icon} <strong>{category_name}:</strong> {explanation}</li>")
        
        return {
            "compliant_items": "<ul>" + "".join(compliant_items) + "</ul>" if compliant_items else "<p>No compliant items identified.</p>",
            "non_compliant_items": "<ul>" + "".join(non_compliant_items) + "</ul>" if non_compliant_items else "<p>No non-compliant items identified.</p>",
            "error": f"AI summary failed, using fallback: {str(e)}"
        }

def generate_html_email(results_data, ai_summary):
    """
    Generate HTML email content with Snowflake blue theme.
    
    Returns:
        str: Complete HTML email content
    """
    # Extract file information
    files_analyzed = results_data.get('files_analyzed', [])
    file1_name = files_analyzed[0].get('file_name', 'Unknown') if len(files_analyzed) > 0 else 'Unknown'
    file2_name = files_analyzed[1].get('file_name', 'Unknown') if len(files_analyzed) > 1 else 'Unknown'
    
    model_used = results_data.get('model_used', 'Unknown')
    timestamp = results_data.get('timestamp', datetime.now().isoformat())
    
    # Prepare table data
    table_rows = ""
    for category, details in results_data.get('results', {}).items():
        evaluation = details.get('evaluation', {})
        score = evaluation.get('evaluation_score', 0)
        explanation = evaluation.get('evaluation_explanation', 'No explanation available')
        
        # Color coding based on score
        score_color = "#28a745" if score >= 0.75 else "#dc3545" if score < 0.5 else "#ffc107"
        
        table_rows += f"""
        <tr>
            <td style="border: solid 1px #DDEEEE; color: #333; padding: 12px; text-shadow: 1px 1px 1px #fff; font-weight: 500;">
                {category.replace('_', ' ').title()}
            </td>
            <td style="border: solid 1px #DDEEEE; color: {score_color}; padding: 12px; text-shadow: 1px 1px 1px #fff; font-weight: bold; text-align: center;">
                {score:.2f}
            </td>
            <td style="border: solid 1px #DDEEEE; color: #333; padding: 12px; text-shadow: 1px 1px 1px #fff; line-height: 1.4;">
                {explanation}
            </td>
        </tr>
        """
    
    # Generate complete HTML email
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Document Validation Results</title>
    </head>
    <body style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; line-height: 1.6; color: #333; max-width: 800px; margin: 0 auto; padding: 20px; background-color: #f8f9fa;">
        
        <!-- Header -->
        <div style="background: linear-gradient(135deg, #29b5e8 0%, #11567f 100%); color: white; padding: 30px; border-radius: 10px 10px 0 0; text-align: center; margin-bottom: 0;">
            <h1 style="margin: 0; font-size: 28px; font-weight: bold; text-shadow: 0 2px 4px rgba(0,0,0,0.3);">
                ❄️ Document Validation Results
            </h1>
            <p style="margin: 10px 0 0 0; font-size: 16px; opacity: 0.9;">
                Powered by Snowflake Cortex AI
            </p>
        </div>
        
        <!-- Main Content -->
        <div style="background: white; padding: 30px; border-radius: 0 0 10px 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.1);">
            
            <!-- File Information -->
            <div style="background-color: #f1f8ff; border-left: 4px solid #29b5e8; padding: 20px; margin-bottom: 25px; border-radius: 5px;">
                <h2 style="color: #11567f; margin: 0 0 15px 0; font-size: 20px;">📋 Files Analyzed</h2>
                <p style="margin: 8px 0; font-size: 14px;">
                    <strong>1st file used:</strong> <code style="background-color: #e3f2fd; padding: 2px 6px; border-radius: 3px; font-family: 'Courier New', monospace;">{file1_name}</code>
                </p>
                <p style="margin: 8px 0; font-size: 14px;">
                    <strong>2nd file used:</strong> <code style="background-color: #e3f2fd; padding: 2px 6px; border-radius: 3px; font-family: 'Courier New', monospace;">{file2_name}</code>
                </p>
                <p style="margin: 8px 0; font-size: 14px;">
                    <strong>Model used:</strong> <span style="color: #11567f; font-weight: 500;">{model_used}</span>
                </p>
            </div>
            
            <!-- AI Summary -->
            <div style="margin-bottom: 30px;">
                <h2 style="color: #11567f; margin: 0 0 20px 0; font-size: 20px;">📊 Compliance Summary</h2>
                
                <div style="display: flex; flex-wrap: wrap; gap: 20px; margin-bottom: 20px;">
                    <div style="flex: 1; min-width: 300px; background-color: #f8fff8; border: 1px solid #28a745; border-radius: 8px; padding: 15px;">
                        <h3 style="color: #28a745; margin: 0 0 10px 0; font-size: 16px;">Compliant Items</h3>
                        {ai_summary.get('compliant_items', '<p>No compliant items identified.</p>')}
                    </div>
                    
                    <div style="flex: 1; min-width: 300px; background-color: #fff8f8; border: 1px solid #dc3545; border-radius: 8px; padding: 15px;">
                        <h3 style="color: #dc3545; margin: 0 0 10px 0; font-size: 16px;">Non-Compliant Items</h3>
                        {ai_summary.get('non_compliant_items', '<p>No non-compliant items identified.</p>')}
                    </div>
                </div>
            </div>
            
            <!-- Detailed Results Table -->
            <div style="margin-bottom: 30px;">
                <h2 style="color: #11567f; margin: 0 0 20px 0; font-size: 20px;">📈 Detailed Analysis</h2>
                
                <div style="overflow-x: auto;">
                    <table style="width: 100%; border: solid 2px #29b5e8; border-collapse: collapse; border-spacing: 0; font: normal 14px 'Segoe UI', sans-serif; border-radius: 8px; overflow: hidden;">
                        <thead>
                            <tr>
                                <th style="background: linear-gradient(135deg, #29b5e8 0%, #11567f 100%); border: solid 1px #11567f; color: white; padding: 15px; text-align: left; font-weight: bold; font-size: 16px;">
                                    Category
                                </th>
                                <th style="background: linear-gradient(135deg, #29b5e8 0%, #11567f 100%); border: solid 1px #11567f; color: white; padding: 15px; text-align: center; font-weight: bold; font-size: 16px;">
                                    Evaluation Score
                                </th>
                                <th style="background: linear-gradient(135deg, #29b5e8 0%, #11567f 100%); border: solid 1px #11567f; color: white; padding: 15px; text-align: left; font-weight: bold; font-size: 16px;">
                                    Evaluation Explanation
                                </th>
                            </tr>
                        </thead>
                        <tbody>
                            {table_rows}
                        </tbody>
                    </table>
                </div>
            </div>
            
            <!-- Footer -->
            <div style="border-top: 2px solid #e9ecef; padding-top: 20px; text-align: center; color: #6c757d; font-size: 12px;">
                <p style="margin: 0;">
                    Generated on {timestamp} • 
                    <span style="color: #29b5e8; font-weight: 500;">Frostlogic Document Validation System</span>
                </p>
                <p style="margin: 5px 0 0 0;">
                    Powered by <strong style="color: #29b5e8;">Snowflake Cortex AI</strong> 
                </p>
            </div>
            
        </div>
    </body>
    </html>
    """
    
    return html_content.strip()

def send_email_notification(session, email_addresses, subject, html_content, email_integration):
    """
    Send HTML email using Snowflake's SYSTEM$SEND_EMAIL function.
    
    Returns:
        dict: {"success": bool, "message": str, "details": dict}
    """
    try:
        # Join email addresses for the email function
        recipients = ','.join(email_addresses)
        
        # Call SYSTEM$SEND_EMAIL with HTML content type
        email_result = session.sql(
            "CALL SYSTEM$SEND_EMAIL(?, ?, ?, ?, ?)",
            params=[email_integration, recipients, subject, html_content, 'text/html']
        ).collect()
        
        return {
            "success": True,
            "message": f"Email sent successfully to {len(email_addresses)} recipient(s)",
            "details": {
                "recipients": email_addresses,
                "subject": subject,
                "integration_used": email_integration,
                "content_type": "text/html",
                "content_length": len(html_content)
            }
        }
        
    except Exception as e:
        return {
            "success": False,
            "message": f"Failed to send email: {str(e)}",
            "details": {
                "recipients": email_addresses,
                "subject": subject,
                "integration_used": email_integration,
                "error": str(e)
            }
        }

def main_handler(session: Session, results_json, email_addresses, email_integration='ACCOUNT_EMAIL_INTEGRATION', model_name='claude-4-sonnet'):
    """
    Main entry point for the stored procedure.
    
    Returns:
        dict: Success/failure response with details
    """
    try:
        # Step 1: Validate inputs
        validation_result = validate_inputs(session, results_json, email_addresses, email_integration, model_name)
        
        if not validation_result["valid"]:
            return {
                "success": False,
                "error_type": "validation_error",
                "error_message": "Input validation failed",
                "details": {
                    "errors": validation_result["errors"]
                }
            }
        
        parsed_data = validation_result["parsed_data"]
        results_data = parsed_data["results_data"]
        valid_emails = parsed_data["valid_emails"]
        
        # Step 2: Generate AI summary
        ai_summary = generate_ai_summary(session, results_data, model_name)
        
        if ai_summary.get("error"):
            # Log the AI error but continue with fallback
            pass
        
        # Step 3: Generate HTML email content
        html_content = generate_html_email(results_data, ai_summary)
        
        # Step 4: Create email subject with file names
        files_analyzed = results_data.get('files_analyzed', [])
        if len(files_analyzed) >= 2:
            # Extract just filenames for subject
            file1_name = files_analyzed[0].get('file_name', '').split('/')[-1] or 'file1'
            file2_name = files_analyzed[1].get('file_name', '').split('/')[-1] or 'file2'
            subject = f"Document validation results - {file1_name}/{file2_name}"
        else:
            subject = "Document validation results"
        
        # Step 5: Send email
        email_result = send_email_notification(session, valid_emails, subject, html_content, email_integration)
        
        if email_result["success"]:
            return {
                "success": True,
                "message": email_result["message"],
                "details": {
                    "validation": "passed",
                    "ai_summary_generated": ai_summary.get("error") is None,
                    "email_details": email_result["details"],
                    "processing_timestamp": datetime.now().isoformat()
                }
            }
        else:
            return {
                "success": False,
                "error_type": "email_error", 
                "error_message": email_result["message"],
                "details": email_result["details"]
            }
            
    except Exception as e:
        return {
            "success": False,
            "error_type": "system_error",
            "error_message": f"Unexpected error in email procedure: {str(e)}",
            "details": {
                "exception_type": type(e).__name__,
                "exception_message": str(e)
            }
        }

$$;

-- ========================================
-- USAGE EXAMPLES
-- ========================================

-- -- Sample call
-- CALL SEND_VALIDATION_EMAIL(
--     PARSE_JSON('{
--         "analysis_type": "multi_file",
--         "files_analyzed": [
--             {
--                 "file_name": "@MSA_STAGE/healthcareco_coredata_msa_v2024.pdf",
--                 "file_type": "MSA"
--             },
--             {
--                 "file_name": "@SOW_STAGE/healthcareco_coredata_sow_v2024.pdf",
--                 "file_type": "SOW"
--             }
--         ],
--         "model_used": "claude-4-sonnet",
--         "results": {
--             "agreement_duration": {
--                 "evaluation": {
--                     "evaluation_explanation": "The SOW duration is compliant with MSA term limits as both documents specify a 2-year term from October 1, 2024, with no automatic renewal.",
--                     "evaluation_score": 0.8
--                 },
--                 "extraction_question": "What is the duration or term of this agreement?",
--                 "file_answers": {
--                     "@MSA_STAGE/healthcareco_coredata_msa_v2024.pdf": "The agreement has a term of two (2) years from the Effective Date.",
--                     "@SOW_STAGE/healthcareco_coredata_sow_v2024.pdf": "The overall Agreements duration is two (2) years from October 1, 2024, with no automatic renewal."
--                 }
--             },
--             "confidentiality_clause": {
--                 "evaluation": {
--                     "evaluation_explanation": "The SOW confidentiality requirements are fully consistent with MSA standards.",
--                     "evaluation_score": 1.0
--                 },
--                 "extraction_question": "Find any confidentiality or non-disclosure clauses.",
--                 "file_answers": {
--                     "@MSA_STAGE/healthcareco_coredata_msa_v2024.pdf": "The MSA defines Confidential Information as any non-public information disclosed between parties.",
--                     "@SOW_STAGE/healthcareco_coredata_sow_v2024.pdf": "The SOW references confidentiality provisions from the MSA."
--                 }
--             },
--             "payment_terms": {
--                 "evaluation": {
--                     "evaluation_explanation": "The SOW has major non-compliance issues with MSA requirements. The MSA requires 45-day payment terms, but the SOW specifies 60 days.",
--                     "evaluation_score": 0.3
--                 },
--                 "extraction_question": "What are the payment terms?",
--                 "file_answers": {
--                     "@MSA_STAGE/healthcareco_coredata_msa_v2024.pdf": "Client shall pay all undisputed invoices within forty-five (45) days of receipt.",
--                     "@SOW_STAGE/healthcareco_coredata_sow_v2024.pdf": "Monthly invoices due within 60 days of receipt."
--                 }
--             },
--             "standard_rate_per_hour": {
--                 "evaluation": {
--                     "evaluation_explanation": "The SOW hourly rate of $150 USD significantly exceeds the MSA approved standard rate of $95 USD by 58%.",
--                     "evaluation_score": 0.2
--                 },
--                 "extraction_question": "What is the standard hourly rate mentioned in this agreement?",
--                 "file_answers": {
--                     "@MSA_STAGE/healthcareco_coredata_msa_v2024.pdf": "The standard hourly rate is $95 USD per hour.",
--                     "@SOW_STAGE/healthcareco_coredata_sow_v2024.pdf": "$150 USD per hour after the initial three months."
--                 }
--             }
--         },
--         "success": true,
--         "summary": {
--             "average_evaluation_score": 0.74,
--             "file_matches": {
--                 "@MSA_STAGE/healthcareco_coredata_msa_v2024.pdf": 10,
--                 "@SOW_STAGE/healthcareco_coredata_sow_v2024.pdf": 10
--             },
--             "high_evaluation_matches": 7,
--             "total_questions": 10
--         },
--         "timestamp": "2025-07-30T14:59:43.101000-07:00"
--     }'),
--     ARRAY_CONSTRUCT(
--         'VIJAY.KARTHIKEYAN@SNOWFLAKE.COM'
--     ),
--     'ACCOUNT_EMAIL_INTEGRATION',
--     'claude-4-sonnet'
-- );
