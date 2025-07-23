-- Enhanced Frostlogic File Comparison Procedures
USE ROLE FROSTLOGIC_ROLE;
USE DATABASE FROSTLOGIC_DB;
USE SCHEMA PUBLIC;
USE WAREHOUSE FROSTLOGIC_WH;

-- ========================================
-- VALIDATION PROCEDURE
-- ========================================

CREATE OR REPLACE PROCEDURE VALIDATE_FILE_CONFIG(
    FILE_CONFIG VARIANT
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'validate_file_config_handler'
AS
$$
import json
from snowflake.snowpark import Session

def validate_file_config_handler(session: Session, file_config):
    try:
        # Parse file config if it's a string
        if isinstance(file_config, str):
            files = json.loads(file_config)
        else:
            files = file_config

        # Validate basic structure
        if not isinstance(files, list):
            return {"valid": False, "error": "file_config must be a JSON array"}

        if len(files) == 0:
            return {"valid": False, "error": "file_config cannot be empty"}

        if len(files) > 15:
            return {"valid": False, "error": "Maximum 15 files allowed for performance reasons"}

        validated_files = []

        # Validate each file entry
        for i, file_entry in enumerate(files):
            if not isinstance(file_entry, dict):
                return {"valid": False, "error": f"File entry {i+1} must be an object"}

            if "file_name" not in file_entry:
                return {"valid": False, "error": f"File entry {i+1} missing file_name"}

            if "file_type" not in file_entry:
                return {"valid": False, "error": f"File entry {i+1} missing file_type"}

            file_name = file_entry["file_name"].strip()
            file_type = file_entry["file_type"].upper().strip()

            if not file_name:
                return {"valid": False, "error": f"File entry {i+1} has empty file_name"}

            if not file_type:
                return {"valid": False, "error": f"File entry {i+1} has empty file_type"}

            # Check if file exists in the chunk table
            chunk_table = f"{file_type}_CHUNKS"

            try:
                file_check_sql = f"""
                SELECT COUNT(*) as file_count
                FROM {chunk_table}
                WHERE FILE_NAME = '{file_name.replace("'", "''")}'
                """

                file_result = session.sql(file_check_sql).collect()
                if not file_result or file_result[0]["FILE_COUNT"] == 0:
                    return {"valid": False, "error": f"File '{file_name}' not found in {chunk_table}"}
            except Exception as file_error:
                return {"valid": False, "error": f"Error checking file {file_name}: {str(file_error)}"}

            validated_files.append({
                "file_name": file_name,
                "file_type": file_type,
                "chunk_table": chunk_table,
                "search_service": f"{file_type}_SEARCH_SERVICE"
            })

        return {
            "valid": True,
            "validated_files": validated_files,
            "file_count": len(validated_files)
        }

    except Exception as e:
        return {"valid": False, "error": f"Validation error: {str(e)}"}
$$;

-- ========================================
-- ENHANCED COMPARE_FILES PROCEDURE WITH EVALUATION
-- ========================================

CREATE OR REPLACE PROCEDURE COMPARE_FILES(
    FILE_CONFIG VARIANT,
    EVALUATION_EXTRACTION_CONFIG VARIANT,
    MODEL_NAME STRING DEFAULT 'claude-4-sonnet'
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'compare_files_handler'
AS
$$
import json
from snowflake.snowpark import Session

def compare_files_handler(session: Session, file_config, evaluation_extraction_config, model_name: str):
    try:
        # JSON Schema definitions for structured AI responses
        ANSWER_SCHEMA = {
            'type': 'json',
            'schema': {
                'type': 'object',
                'properties': {
                    'answer': {'type': 'string'}
                },
                'required': ['answer'],
                'additionalProperties': False
            }
        }
        
        EVALUATION_SCHEMA = {
            'type': 'json',
            'schema': {
                'type': 'object',
                'properties': {
                    'evaluation_score': {'type': 'number'},
                    'evaluation_explanation': {'type': 'string'}
                },
                'required': ['evaluation_score', 'evaluation_explanation'],
                'additionalProperties': False
            }
        }
        
        # Parse configurations
        if isinstance(file_config, str):
            files = json.loads(file_config)
        else:
            files = file_config
            
        if isinstance(evaluation_extraction_config, str):
            config = json.loads(evaluation_extraction_config)
        else:
            config = evaluation_extraction_config

        # Extract configuration sections
        extraction_config = config.get('extraction_config', {})
        evaluation_config = config.get('evaluation_config', {})
        search_limit = config.get('search_limit', 3)
        
        # Initialize results structure
        comparison_results = {
            "success": True,
            "files_analyzed": files,
            "model_used": model_name,
            "timestamp": session.sql("SELECT CURRENT_TIMESTAMP()").collect()[0][0].isoformat(),
            "analysis_type": "multi_file" if len(files) > 1 else "single_file",
            "results": {},
            "summary": {
                "total_questions": len(extraction_config),
                "file_matches": {file_info['file_name']: 0 for file_info in files},
                "high_evaluation_matches": 0,
                "average_evaluation_score": 0.0
            }
        }
        
        total_evaluation_score = 0.0
        evaluation_count = 0

        # Process each question category
        for category, extraction_question in extraction_config.items():
            try:
                category_results = {
                    "extraction_question": extraction_question,
                    "file_answers": {},
                    "evaluation": {}
                }
                
                file_answers = {}
                file_contexts = {}
                
                # Extract answers from each file
                for file_info in files:
                    file_name = file_info['file_name']
                    file_type = file_info['file_type'].upper()
                    search_service = f"{file_type}_SEARCH_SERVICE"
                    
                    # Extract just the filename without stage prefix for search
                    if '/' in file_name:
                        search_file_name = file_name.split('/')[-1]
                    else:
                        search_file_name = file_name
                    
                    try:
                        # Search for relevant content
                        search_sql = f"""
                        SELECT 
                            value['CHUNK_TEXT']::string as CHUNK_TEXT,
                            value['FILE_NAME']::string as FILE_NAME,
                            value['CHUNK_NUMBER']::int as CHUNK_NUMBER,
                            value['CHUNK_ID']::string as CHUNK_ID
                        FROM TABLE(FLATTEN(PARSE_JSON(SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
                            '{search_service}',
                            '{{"query": "{extraction_question.replace("'", "''")}",
                              "columns": ["CHUNK_TEXT", "FILE_NAME", "CHUNK_NUMBER", "CHUNK_ID"],
                              "filter": {{"@eq": {{"FILE_NAME": "{search_file_name}"}}}},
                              "limit": {search_limit}}}'
                        ))['results']))
                        """
                        
                        search_results = session.sql(search_sql).collect()
                        
                        if search_results:
                            contexts = [{"text": row["CHUNK_TEXT"], "file": row["FILE_NAME"]} for row in search_results]
                            file_contexts[file_name] = contexts
                            
                            # Generate structured answer using JSON schema
                            answer = generate_structured_answer(session, extraction_question, contexts, file_type, model_name, ANSWER_SCHEMA)
                            file_answers[file_name] = answer
                            
                            if answer.lower() not in ['not found', 'not specified', 'n/a']:
                                comparison_results["summary"]["file_matches"][file_name] += 1
                        else:
                            file_answers[file_name] = "Not found in document"
                            file_contexts[file_name] = []
                            
                    except Exception as e:
                        file_answers[file_name] = f"Error: {str(e)[:100]}"
                        file_contexts[file_name] = []
                
                category_results["file_answers"] = file_answers
                
                # Perform evaluation if we have multiple files and evaluation config
                if len(files) >= 2 and category in evaluation_config:
                    evaluation_question = evaluation_config[category]
                    
                    # Check if we have valid answers for evaluation
                    valid_answers = [answer for answer in file_answers.values() 
                                   if answer.lower() not in ['not found', 'not specified', 'n/a', 'error']]
                    
                    if len(valid_answers) >= 2:
                        try:
                            # Generate evaluation using structured JSON schema
                            evaluation_result = generate_evaluation(
                                session, evaluation_question, file_answers, files, model_name, EVALUATION_SCHEMA
                            )
                            
                            category_results["evaluation"] = evaluation_result
                            
                            # Track evaluation metrics
                            score = evaluation_result.get('evaluation_score', 0.0)
                            total_evaluation_score += score
                            evaluation_count += 1
                            
                            if score >= 0.8:
                                comparison_results["summary"]["high_evaluation_matches"] += 1
                                
                        except Exception as e:
                            category_results["evaluation"] = {
                                "evaluation_score": 0.0,
                                "evaluation_explanation": f"Error in evaluation: {str(e)[:100]}"
                            }
                    else:
                        category_results["evaluation"] = {
                            "evaluation_score": 0.0,
                            "evaluation_explanation": "Insufficient valid data for evaluation"
                        }
                else:
                    category_results["evaluation"] = {
                        "evaluation_score": 0.0,
                        "evaluation_explanation": "Single file analysis - no evaluation performed"
                    }
                
                # Add context metadata
                category_results["context_metadata"] = {
                    file_name: {"context_count": len(contexts)} 
                    for file_name, contexts in file_contexts.items()
                }
                
                comparison_results["results"][category] = category_results
                
            except Exception as e:
                comparison_results["results"][category] = {
                    "error": str(e),
                    "file_answers": {},
                    "evaluation": {
                        "evaluation_score": 0.0,
                        "evaluation_explanation": "Error processing category"
                    }
                }
        
        # Calculate summary statistics
        if evaluation_count > 0:
            comparison_results["summary"]["average_evaluation_score"] = total_evaluation_score / evaluation_count
        
        return comparison_results
        
    except Exception as e:
        return {"error": f"Comparison failed: {str(e)}"}

def generate_structured_answer(session: Session, question: str, contexts: list, doc_type: str, model_name: str, schema: dict):
    """Generate a structured answer using JSON schema"""
    try:
        if not contexts:
            return "Not found"
        
        context_text = "\n\n".join([f"{ctx['file']}: {ctx['text']}" for ctx in contexts])
        
        prompt = f"""Based on the provided context from {doc_type} document, answer this question briefly and concisely:

Context:
{context_text}

Question: {question}

Provide a brief, direct answer. If the information is not found, respond with "Not specified" or "Not found".
Keep the answer under 100 words and focus on factual information only."""
        
        answer_sql = f"""
        SELECT AI_COMPLETE(
            model => '{model_name}',
            prompt => '{prompt.replace("'", "''")}',
            response_format => OBJECT_CONSTRUCT(
                'type', 'json',
                'schema', OBJECT_CONSTRUCT(
                    'type', 'object',
                    'properties', OBJECT_CONSTRUCT(
                        'answer', OBJECT_CONSTRUCT('type', 'string')
                    ),
                    'required', ARRAY_CONSTRUCT('answer'),
                    'additionalProperties', false
                )
            )
        ) as answer
        """
        
        result = session.sql(answer_sql).collect()
        if result:
            answer_json = result[0]["ANSWER"]
            
            # Parse structured JSON response
            if isinstance(answer_json, str):
                answer_data = json.loads(answer_json)
            else:
                answer_data = answer_json
            
            answer = answer_data.get('answer', 'Not found').strip()
            return answer if answer else "Not found"
        return "Not found"
        
    except Exception as e:
        return f"Error: {str(e)}"

def generate_evaluation(session: Session, evaluation_question: str, file_answers: dict, files: list, model_name: str, schema: dict):
    """Generate evaluation with scores using structured JSON schema"""
    try:
        # Prepare file answers summary
        answers_summary = "\n".join([
            f"{file_info['file_name']} ({file_info['file_type']}): {file_answers.get(file_info['file_name'], 'No answer')}"
            for file_info in files
        ])
        
        prompt = f"""Evaluate whether the contract terms meet the specified criteria and provide an evaluation score.

Evaluation Criteria: {evaluation_question}

File Responses:
{answers_summary}

Provide an evaluation score from 0.0-1.0 based on how well the criteria is met:
- 1.0: Fully compliant/aligned
- 0.8-0.9: Mostly compliant with minor gaps
- 0.6-0.7: Partially compliant with notable issues
- 0.4-0.5: Significant compliance gaps
- 0.0-0.3: Major non-compliance or conflicts

Also provide a brief explanation (maximum 100 words) of the evaluation logic."""
        
        evaluation_sql = f"""
        SELECT AI_COMPLETE(
            model => '{model_name}',
            prompt => '{prompt.replace("'", "''")}',
            response_format => OBJECT_CONSTRUCT(
                'type', 'json',
                'schema', OBJECT_CONSTRUCT(
                    'type', 'object',
                    'properties', OBJECT_CONSTRUCT(
                        'evaluation_score', OBJECT_CONSTRUCT('type', 'number'),
                        'evaluation_explanation', OBJECT_CONSTRUCT('type', 'string')
                    ),
                    'required', ARRAY_CONSTRUCT('evaluation_score', 'evaluation_explanation'),
                    'additionalProperties', false
                )
            )
        ) as evaluation_result
        """
        
        result = session.sql(evaluation_sql).collect()
        if result:
            evaluation_json = result[0]["EVALUATION_RESULT"]
            
            # Parse structured JSON response
            if isinstance(evaluation_json, str):
                evaluation_data = json.loads(evaluation_json)
            else:
                evaluation_data = evaluation_json
            
            score = float(evaluation_data.get('evaluation_score', 0.0))
            score = min(max(score, 0.0), 1.0)  # Ensure within bounds
            
            explanation = evaluation_data.get('evaluation_explanation', 'No evaluation provided')[:400]
            
            return {
                "evaluation_score": score,
                "evaluation_explanation": explanation
            }
        
        return {
            "evaluation_score": 0.0,
            "evaluation_explanation": "No evaluation result generated"
        }
        
    except Exception as e:
        return {
            "evaluation_score": 0.0,
            "evaluation_explanation": f"Error in evaluation: {str(e)}"
        }
$$;

-- Create the main document comparison procedure using AI_EXTRACT
CREATE OR REPLACE PROCEDURE COMPARE_FILES_AISQL(
    FILE_CONFIG VARIANT,
    EVALUATION_EXTRACTION_CONFIG VARIANT,
    MODEL_NAME STRING DEFAULT 'claude-4-sonnet'
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'compare_files_aisql_handler'
AS
$$
import json
from snowflake.snowpark import Session

def compare_files_aisql_handler(session: Session, file_config, evaluation_extraction_config, model_name: str):
    try:
        # Parse configurations
        if isinstance(file_config, str):
            files = json.loads(file_config)
        else:
            files = file_config
            
        if isinstance(evaluation_extraction_config, str):
            config = json.loads(evaluation_extraction_config)
        else:
            config = evaluation_extraction_config

        # Extract configuration sections
        extraction_config = config.get('extraction_config', {})
        evaluation_config = config.get('evaluation_config', {})
        search_limit = config.get('search_limit', 3)
        
        # Initialize results structure to match COMPARE_FILES output
        comparison_results = {
            "success": True,
            "files_analyzed": files,
            "model_used": model_name,
            "timestamp": session.sql("SELECT CURRENT_TIMESTAMP()").collect()[0][0].isoformat(),
            "analysis_type": "multi_file" if len(files) > 1 else "single_file",
            "extraction_method": "AI_EXTRACT_BULK",
            "results": {},
            "summary": {
                "total_questions": len(extraction_config),
                "file_matches": {file_info['file_name']: 0 for file_info in files},
                "high_evaluation_matches": 0,
                "average_evaluation_score": 0.0
            }
        }
        
        # Build responseFormat array for AI_EXTRACT
        response_format = []
        for field_name, question in extraction_config.items():
            response_format.append([field_name, question])
        
        # Extract from each file using AI_EXTRACT
        file_extractions = {}
        for file_info in files:
            file_name = file_info['file_name']
            file_type = file_info['file_type']
            
            try:
                # Build AI_EXTRACT SQL - format file path for TO_FILE function
                file_path = file_name
                if not file_path.startswith('@'):
                    file_path = f'@{file_path}'
                
                extract_sql = f"""
                SELECT AI_EXTRACT(
                    file => TO_FILE('{file_path}'),
                    responseFormat => {response_format}
                ) as extraction_result
                """
                
                result = session.sql(extract_sql).collect()
                if result and result[0]["EXTRACTION_RESULT"]:
                    if isinstance(result[0]["EXTRACTION_RESULT"], str):
                        extraction = json.loads(result[0]["EXTRACTION_RESULT"])
                    else:
                        extraction = result[0]["EXTRACTION_RESULT"]
                    
                    file_extractions[file_name] = extraction
                    
                    # Count successful extractions for this file
                    if "response" in extraction:
                        successful_extractions = len([v for v in extraction["response"].values() 
                                                    if v and str(v).strip().lower() not in ['', 'not found', 'not specified', 'n/a']])
                        comparison_results["summary"]["file_matches"][file_name] = successful_extractions
                    
            except Exception as e:
                file_extractions[file_name] = {"error": f"Extraction failed: {str(e)}"}
                comparison_results["summary"]["file_matches"][file_name] = 0
        
        # Process each question category to match COMPARE_FILES output format
        total_evaluation_score = 0.0
        evaluation_count = 0
        
        for category, extraction_question in extraction_config.items():
            try:
                category_results = {
                    "extraction_question": extraction_question,
                    "file_answers": {},
                    "evaluation": {},
                    "context_metadata": {}
                }
                
                # Extract answers for this category from each file
                file_answers = {}
                for file_info in files:
                    file_name = file_info['file_name']
                    
                    # Get the answer from AI_EXTRACT results
                    answer = "Not found in document"
                    if (file_name in file_extractions and 
                        "response" in file_extractions[file_name] and 
                        category in file_extractions[file_name]["response"]):
                        
                        extracted_value = file_extractions[file_name]["response"][category]
                        if extracted_value and str(extracted_value).strip():
                            answer = str(extracted_value).strip()
                    
                    file_answers[file_name] = answer
                    
                    # Add context metadata
                    category_results["context_metadata"][file_name] = {
                        "context_count": 1 if answer != "Not found in document" else 0
                    }
                
                category_results["file_answers"] = file_answers
                
                # Perform evaluation if we have multiple files and evaluation config
                if len(files) >= 2 and category in evaluation_config:
                    evaluation_question = evaluation_config[category]
                    
                    # Check if we have valid answers for evaluation
                    valid_answers = [answer for answer in file_answers.values() 
                                   if answer.lower() not in ['not found in document', 'not found', 'not specified', 'n/a']]
                    
                    if len(valid_answers) >= 2:
                        try:
                            # Prepare file answers summary for evaluation
                            answers_summary = "\n".join([
                                f"{file_info['file_name']} ({file_info['file_type']}): {file_answers.get(file_info['file_name'], 'No answer')}"
                                for file_info in files
                            ])
                            
                            evaluation_prompt = f"""Evaluate whether the contract terms meet the specified criteria and provide an evaluation score.

Evaluation Criteria: {evaluation_question}

File Responses:
{answers_summary}

Provide an evaluation score from 0.0-1.0 based on how well the criteria is met:
- 1.0: Fully compliant/aligned
- 0.8-0.9: Mostly compliant with minor gaps
- 0.6-0.7: Partially compliant with notable issues
- 0.4-0.5: Significant compliance gaps
- 0.0-0.3: Major non-compliance or conflicts

Also provide a brief explanation (maximum 100 words) of the evaluation logic."""
                            
                            # Clean prompt for SQL
                            safe_prompt = evaluation_prompt.replace("'", "''")
                            
                            evaluation_sql = f"""
                            SELECT AI_COMPLETE(
                                model => '{model_name}',
                                prompt => '{safe_prompt}',
                                response_format => OBJECT_CONSTRUCT(
                                    'type', 'json',
                                    'schema', OBJECT_CONSTRUCT(
                                        'type', 'object',
                                        'properties', OBJECT_CONSTRUCT(
                                            'evaluation_score', OBJECT_CONSTRUCT('type', 'number'),
                                            'evaluation_explanation', OBJECT_CONSTRUCT('type', 'string')
                                        ),
                                        'required', ARRAY_CONSTRUCT('evaluation_score', 'evaluation_explanation'),
                                        'additionalProperties', false
                                    )
                                )
                            ) as evaluation_result
                            """
                            
                            eval_result = session.sql(evaluation_sql).collect()
                            if eval_result and eval_result[0]["EVALUATION_RESULT"]:
                                evaluation_response = eval_result[0]["EVALUATION_RESULT"]
                                
                                # Parse structured JSON response
                                if isinstance(evaluation_response, str):
                                    evaluation_data = json.loads(evaluation_response)
                                else:
                                    evaluation_data = evaluation_response
                                
                                score = float(evaluation_data.get('evaluation_score', 0.0))
                                score = min(max(score, 0.0), 1.0)  # Ensure within bounds
                                
                                explanation = evaluation_data.get('evaluation_explanation', 'No evaluation provided')[:400]
                                
                                category_results["evaluation"] = {
                                    "evaluation_score": score,
                                    "evaluation_explanation": explanation
                                }
                                
                                total_evaluation_score += score
                                evaluation_count += 1
                                
                                # Track high evaluation matches
                                if score >= 0.8:
                                    comparison_results["summary"]["high_evaluation_matches"] += 1
                                    
                        except Exception as e:
                            category_results["evaluation"] = {
                                "evaluation_score": 0.0,
                                "evaluation_explanation": f"Error in evaluation: {str(e)[:100]}"
                            }
                else:
                    # Single file analysis - no evaluation
                    category_results["evaluation"] = {
                        "evaluation_score": 0.0,
                        "evaluation_explanation": "Single file analysis - no evaluation performed"
                    }
                
                # Store results for this category
                comparison_results["results"][category] = category_results
                
            except Exception as e:
                comparison_results["results"][category] = {
                    "extraction_question": extraction_question,
                    "file_answers": {file_info['file_name']: f"Error: {str(e)}" for file_info in files},
                    "evaluation": {
                        "evaluation_score": 0.0,
                        "evaluation_explanation": f"Error processing category: {str(e)[:100]}"
                    },
                    "context_metadata": {}
                }
        
        # Calculate average evaluation score
        if evaluation_count > 0:
            comparison_results["summary"]["average_evaluation_score"] = total_evaluation_score / evaluation_count
        
        # Store raw extraction results for debugging
        comparison_results["raw_extractions"] = file_extractions
        
        return comparison_results
        
    except Exception as e:
        return {"error": f"Procedure execution error: {str(e)}", "success": False}

$$;

-- ========================================
-- HELPER PROCEDURES
-- ========================================
CREATE OR REPLACE PROCEDURE GET_AVAILABLE_MODELS()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
BEGIN
    RETURN OBJECT_CONSTRUCT(
        'supported_models', ARRAY_CONSTRUCT(
            'claude-4-sonnet',
            'claude-3-7-sonnet',
            'claude-3-5-sonnet',
            'llama4-maverick',
            'llama3-8b',
            'llama3-70b', 
            'llama3.1-8b',
            'llama3.1-70b',
            'llama3.1-405b',
            'openai-gpt-4.1',
            'openai-o4-mini',
            'snowflake-llama-3.1-405b',
            'snowflake-arctic',
            'deepseek-r1',
            'reka-core',
            'reka-flash',
            'mixtral-8x7b',
            'mistral-large',
            'mistral-7b',
            'gemma-7b',
            'jamba-instruct'
        ),
        'default_model', 'claude-4-sonnet',
        'recommended_models', OBJECT_CONSTRUCT(
            'speed', 'llama3-8b',
            'balanced', 'mixtral-8x7b', 
            'quality', 'claude-4-sonnet',
            'reasoning', 'claude-4-sonnet',
            'enterprise', 'claude-4-sonnet'
        )
    );
END;
$$;
