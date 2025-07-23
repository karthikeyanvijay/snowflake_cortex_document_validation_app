-- ========================================
-- FROSTLOGIC COMPREHENSIVE API TESTING
-- ========================================

USE ROLE FROSTLOGIC_ROLE;
USE DATABASE FROSTLOGIC_DB;
USE SCHEMA PUBLIC;
USE WAREHOUSE FROSTLOGIC_WH;


-- ========================================
-- TEST 1: COMPREHENSIVE ANALYSIS (MSA + SOW)
-- ========================================

SELECT '=== TEST 1: Comprehensive Contract Analysis - All 10 Parameters (claude-4-sonnet) ===' as INFO;

CALL COMPARE_FILES(
    FILE_CONFIG => PARSE_JSON('[
        {"file_name": "MSA_STAGE/healthcareco_coredata_msa_v2024.pdf", "file_type": "MSA"},
        {"file_name": "SOW_STAGE/healthcareco_coredata_sow_v2024.pdf", "file_type": "SOW"}
    ]'),
    EVALUATION_EXTRACTION_CONFIG => PARSE_JSON('{
        "extraction_config": {
            "effective_date": "What is the effective date or start date of this agreement? Look for specific dates when the contract becomes effective.",
            "agreement_duration": "What is the duration or term of this agreement? Look for how long the contract lasts (e.g., 1 year, 2 years, etc.).",
            "notice_period": "What is the notice period required for termination? Look for how much advance notice is needed to terminate the agreement.",
            "payment_terms": "What are the payment terms? Look for information about when and how payments should be made (e.g., net 30, monthly, etc.).",
            "standard_rate_per_hour": "What is the standard hourly rate or rate per hour mentioned in this agreement? Look for specific dollar amounts per hour.",
            "force_majeure_clause": "Find any force majeure clauses. Extract the specific text that discusses force majeure, acts of God, or circumstances beyond control.",
            "indemnification_clause": "Find any indemnification clauses. Extract specific text about indemnification, liability protection, or holding harmless provisions.",
            "renewal_options_clause": "Find any renewal or extension clauses. Extract text about contract renewal options, automatic renewals, or extension terms.",
            "confidentiality_clause": "Find any confidentiality or non-disclosure clauses. Extract specific text about confidential information, NDAs, or privacy requirements.",
            "data_security_clause": "Find any data security or data protection clauses. Extract text about data security requirements, data protection measures, or security standards."
        },
        "evaluation_config": {
            "effective_date": "Does the SOW start date fall within the MSA effective period and comply with master agreement timing requirements?",
            "agreement_duration": "Is the SOW duration within the MSA term limits and does it comply with maximum allowable project durations?",
            "notice_period": "Are the SOW termination notice periods consistent with MSA requirements and governance procedures?",
            "payment_terms": "Do the SOW payment terms fully comply with MSA payment requirements and approved financial procedures?",
            "standard_rate_per_hour": "Are the SOW hourly rates within the MSA approved rate structure and do they comply with pricing guidelines?",
            "force_majeure_clause": "Do the SOW force majeure provisions align with MSA requirements for scope, coverage, and procedural compliance?",
            "indemnification_clause": "Are the SOW indemnification terms consistent with MSA requirements regarding liability allocation and protection scope?",
            "renewal_options_clause": "Do the SOW renewal terms comply with MSA requirements regarding extension periods, conditions, and approval processes?",
            "confidentiality_clause": "Are the SOW confidentiality requirements consistent with MSA standards for information protection and disclosure restrictions?",
            "data_security_clause": "Do the SOW data security standards meet or exceed the MSA requirements for data protection and privacy compliance?"
        },
        "search_limit": 2
    }'),
    MODEL_NAME => 'claude-4-sonnet'
);

-- ========================================
-- TEST 2: SINGLE FILE EXTRACTION (MSA ONLY)
-- ========================================

SELECT '=== TEST 2: Single File MSA Extraction - All 10 Parameters (claude-4-sonnet) ===' as INFO;

CALL COMPARE_FILES(
    FILE_CONFIG => PARSE_JSON('[
        {"file_name": "MSA_STAGE/healthcareco_coredata_msa_v2024.pdf", "file_type": "MSA"}
    ]'),
    EVALUATION_EXTRACTION_CONFIG => PARSE_JSON('{
        "extraction_config": {
            "effective_date": "What is the effective date or start date of this agreement? Look for specific dates when the contract becomes effective.",
            "agreement_duration": "What is the duration or term of this agreement? Look for how long the contract lasts (e.g., 1 year, 2 years, etc.).",
            "notice_period": "What is the notice period required for termination? Look for how much advance notice is needed to terminate the agreement.",
            "payment_terms": "What are the payment terms? Look for information about when and how payments should be made (e.g., net 30, monthly, etc.).",
            "standard_rate_per_hour": "What is the standard hourly rate or rate per hour mentioned in this agreement? Look for specific dollar amounts per hour.",
            "force_majeure_clause": "Find any force majeure clauses. Extract the specific text that discusses force majeure, acts of God, or circumstances beyond control.",
            "indemnification_clause": "Find any indemnification clauses. Extract specific text about indemnification, liability protection, or holding harmless provisions.",
            "renewal_options_clause": "Find any renewal or extension clauses. Extract text about contract renewal options, automatic renewals, or extension terms.",
            "confidentiality_clause": "Find any confidentiality or non-disclosure clauses. Extract specific text about confidential information, NDAs, or privacy requirements.",
            "data_security_clause": "Find any data security or data protection clauses. Extract text about data security requirements, data protection measures, or security standards."
        },
        "search_limit": 2
    }'),
    MODEL_NAME => 'claude-4-sonnet'
);


-- Test 3 the procedure with the same format as COMPARE_FILES
CALL COMPARE_FILES_AISQL(
    FILE_CONFIG => PARSE_JSON('[
        {"file_name": "MSA_STAGE/healthcareco_coredata_msa_v2024.pdf", "file_type": "MSA"},
        {"file_name": "SOW_STAGE/healthcareco_coredata_sow_v2024.pdf", "file_type": "SOW"}
    ]'),
    EVALUATION_EXTRACTION_CONFIG => PARSE_JSON('{
        "extraction_config": {
            "effective_date": "What is the effective date or start date of this agreement? Look for specific dates when the contract becomes effective.",
            "agreement_duration": "What is the duration or term of this agreement? Look for how long the contract lasts (e.g., 1 year, 2 years, etc.).",
            "notice_period": "What is the notice period required for termination? Look for how much advance notice is needed to terminate the agreement.",
            "payment_terms": "What are the payment terms? Look for information about when and how payments should be made (e.g., net 30, monthly, etc.).",
            "standard_rate_per_hour": "What is the standard hourly rate or rate per hour mentioned in this agreement? Look for specific dollar amounts per hour.",
            "force_majeure_clause": "Find any force majeure clauses. Extract the specific text that discusses force majeure, acts of God, or circumstances beyond control.",
            "indemnification_clause": "Find any indemnification clauses. Extract specific text about indemnification, liability protection, or holding harmless provisions.",
            "renewal_options_clause": "Find any renewal or extension clauses. Extract text about contract renewal options, automatic renewals, or extension terms.",
            "confidentiality_clause": "Find any confidentiality or non-disclosure clauses. Extract specific text about confidential information, NDAs, or privacy requirements.",
            "data_security_clause": "Find any data security or data protection clauses. Extract text about data security requirements, data protection measures, or security standards."
        },
        "evaluation_config": {
            "effective_date": "Does the SOW start date fall within the MSA effective period and comply with master agreement timing requirements?",
            "agreement_duration": "Is the SOW duration within the MSA term limits and does it comply with maximum allowable project durations?",
            "notice_period": "Are the SOW termination notice periods consistent with MSA requirements and governance procedures?",
            "payment_terms": "Do the SOW payment terms fully comply with MSA payment requirements and approved financial procedures?",
            "standard_rate_per_hour": "Are the SOW hourly rates within the MSA approved rate structure and do they comply with pricing guidelines?",
            "force_majeure_clause": "Do the SOW force majeure provisions align with MSA requirements for scope, coverage, and procedural compliance?",
            "indemnification_clause": "Are the SOW indemnification terms consistent with MSA requirements regarding liability allocation and protection scope?",
            "renewal_options_clause": "Do the SOW renewal terms comply with MSA requirements regarding extension periods, conditions, and approval processes?",
            "confidentiality_clause": "Are the SOW confidentiality requirements consistent with MSA standards for information protection and disclosure restrictions?",
            "data_security_clause": "Do the SOW data security standards meet or exceed the MSA requirements for data protection and privacy compliance?"
        },
        "search_limit": 5
    }'),
    MODEL_NAME => 'claude-4-sonnet'
); 

-- Test 4 case for single file analysis (no comparison/evaluation)
SELECT '=== TEST 2: Single File Analysis - MSA Document Only (AI_EXTRACT) ===' as INFO;

CALL COMPARE_FILES_AISQL(
    FILE_CONFIG => PARSE_JSON('[
        {"file_name": "MSA_STAGE/healthcareco_coredata_msa_v2024.pdf", "file_type": "MSA"}
    ]'),
    EVALUATION_EXTRACTION_CONFIG => PARSE_JSON('{
        "extraction_config": {
            "effective_date": "What is the effective date or start date of this agreement? Look for specific dates when the contract becomes effective.",
            "agreement_duration": "What is the duration or term of this agreement? Look for how long the contract lasts (e.g., 1 year, 2 years, etc.).",
            "notice_period": "What is the notice period required for termination? Look for how much advance notice is needed to terminate the agreement.",
            "payment_terms": "What are the payment terms? Look for information about when and how payments should be made (e.g., net 30, monthly, etc.).",
            "standard_rate_per_hour": "What is the standard hourly rate or rate per hour mentioned in this agreement? Look for specific dollar amounts per hour.",
            "force_majeure_clause": "Find any force majeure clauses. Extract the specific text that discusses force majeure, acts of God, or circumstances beyond control.",
            "indemnification_clause": "Find any indemnification clauses. Extract specific text about indemnification, liability protection, or holding harmless provisions.",
            "renewal_options_clause": "Find any renewal or extension clauses. Extract text about contract renewal options, automatic renewals, or extension terms.",
            "confidentiality_clause": "Find any confidentiality or non-disclosure clauses. Extract specific text about confidential information, NDAs, or privacy requirements.",
            "data_security_clause": "Find any data security or data protection clauses. Extract text about data security requirements, data protection measures, or security standards."
        },
        "evaluation_config": {
            "effective_date": "Single file analysis - no evaluation criteria needed",
            "agreement_duration": "Single file analysis - no evaluation criteria needed",
            "notice_period": "Single file analysis - no evaluation criteria needed",
            "payment_terms": "Single file analysis - no evaluation criteria needed",
            "standard_rate_per_hour": "Single file analysis - no evaluation criteria needed",
            "force_majeure_clause": "Single file analysis - no evaluation criteria needed",
            "indemnification_clause": "Single file analysis - no evaluation criteria needed",
            "renewal_options_clause": "Single file analysis - no evaluation criteria needed",
            "confidentiality_clause": "Single file analysis - no evaluation criteria needed",
            "data_security_clause": "Single file analysis - no evaluation criteria needed"
        },
        "search_limit": 5
    }'),
    MODEL_NAME => 'claude-4-sonnet'
); 