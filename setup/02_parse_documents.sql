-- Comprehensive Frostlogic File Processing Setup
-- ENHANCED FEATURES:
-- • PARSE_DOCUMENT with LAYOUT mode for better table/structure extraction  
-- • Support for both PDF and DOCX files
-- • Removed length filtering for comprehensive content capture

USE ROLE FROSTLOGIC_ROLE;
USE DATABASE FROSTLOGIC_DB;
USE SCHEMA PUBLIC;
USE WAREHOUSE FROSTLOGIC_WH;

-- Create stored procedure for file processing logic (called by tasks)
CREATE OR REPLACE PROCEDURE PROCESS_FILES_FROM_STREAM(
    FILE_TYPE STRING,
    CHUNK_SIZE INTEGER DEFAULT 1000,
    CHUNK_OVERLAP INTEGER DEFAULT 100,
    TARGET_LAG STRING DEFAULT '1 minute'
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'process_files_handler'
AS
$$
import json
from snowflake.snowpark import Session

def process_files_handler(session: Session, file_type: str, chunk_size: int = 1000, chunk_overlap: int = 100, target_lag: str = '1 minute'):
    try:
        if not file_type or not file_type.strip():
            return {"error": "File type cannot be empty"}
        
        file_type = file_type.upper().strip()
        
        # Generate object names from file type
        stage_name = f"{file_type}_STAGE"
        stream_name = f"{file_type}_STREAM"
        chunks_table_name = f"{file_type}_CHUNKS"
        search_service_name = f"{file_type}_SEARCH_SERVICE"
        
                # Initialize counters
        files_processed = 0
        chunks_created = 0

        # Step 1: Consume stream by creating temporary table (this properly consumes stream records)
        import time
        temp_table_name = f"TEMP_{file_type}_STREAM_DATA_{int(time.time() * 1000000)}"
        
        # Create transient table to consume stream (cleaned up at end, no fail-safe costs)
        consume_stream_sql = f"""
        CREATE OR REPLACE TRANSIENT TABLE {temp_table_name}
        DATA_RETENTION_TIME_IN_DAYS = 0 AS
        SELECT 
            s.RELATIVE_PATH,
            s.METADATA$ACTION,
            s.METADATA$ISUPDATE
        FROM {stream_name} s
        WHERE s.METADATA$ACTION = 'INSERT'
        AND (UPPER(s.RELATIVE_PATH) LIKE '%.PDF' OR UPPER(s.RELATIVE_PATH) LIKE '%.DOCX')
        """
        
        session.sql(consume_stream_sql).collect()
        
        # Step 2: Process files from temp table using SELECT + RESULT_SCAN pattern to avoid internal errors
        select_sql = f"""
        SELECT 
            REGEXP_REPLACE(t.RELATIVE_PATH, '.*/([^/]+)$', '\\\\1') as file_name,
            t.RELATIVE_PATH as full_path,
            '{stage_name}' as stage_name,
            c.index + 1 as chunk_number,
            c.value::STRING as chunk_text
        FROM 
            {temp_table_name} t,
            LATERAL FLATTEN(
                SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER(
                    SNOWFLAKE.CORTEX.PARSE_DOCUMENT(
                        '@{stage_name}',
                        t.RELATIVE_PATH,
                        {{'mode': 'LAYOUT'}}
                    ):content::STRING,
                    'markdown',
                    {chunk_size},
                    {chunk_overlap}
                )
            ) c
        """
        
        # Execute SELECT to generate results
        session.sql(select_sql).collect()
        
        # Step 3: Insert from result scan
        insert_sql = f"""
        INSERT INTO {chunks_table_name} (FILE_NAME, FULL_PATH, STAGE_NAME, CHUNK_NUMBER, CHUNK_TEXT)
        SELECT file_name, full_path, stage_name, chunk_number, chunk_text
        FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
        """
        
        session.sql(insert_sql).collect()
        
        # Step 4: Clean up temp table  
        try:
            session.sql(f"DROP TABLE {temp_table_name}").collect()
        except Exception as e:
            # Log but don't fail if cleanup fails
            pass
        
        # Get count of files processed
        files_count_sql = f"""
        SELECT COUNT(DISTINCT RELATIVE_PATH) as file_count
        FROM {stream_name}
        WHERE METADATA$ACTION = 'INSERT' AND (UPPER(RELATIVE_PATH) LIKE '%.PDF' OR UPPER(RELATIVE_PATH) LIKE '%.DOCX')
        """
        
        files_result = session.sql(files_count_sql).collect()
        files_processed = files_result[0]["FILE_COUNT"] if files_result else 0
        
        # Get count of chunks created
        chunks_count_sql = f"""
        SELECT COUNT(*) as chunk_count
        FROM {chunks_table_name}
        WHERE CREATED_AT >= CURRENT_TIMESTAMP() - INTERVAL '2 minutes'
        """
        
        chunks_result = session.sql(chunks_count_sql).collect()
        chunks_created = chunks_result[0]["CHUNK_COUNT"] if chunks_result else 0
        
        return {
            "success": True,
            "file_type": file_type,
            "stage_name": stage_name,
            "stream_name": stream_name,
            "chunks_table_name": chunks_table_name,
            "search_service_name": search_service_name,
            "files_processed": files_processed,
            "chunks_created": chunks_created,
            "message": f"Processed {files_processed} files, created {chunks_created} chunks"
        }
        
    except Exception as e:
        return {"error": f"File processing failed: {str(e)}"}
$$;

-- Create comprehensive procedure to setup complete file processing pipeline
CREATE OR REPLACE PROCEDURE SETUP_FILE_PROCESSING_PIPELINE(
    FILE_TYPE STRING,
    CHUNK_SIZE INTEGER DEFAULT 1000,
    CHUNK_OVERLAP INTEGER DEFAULT 100,
    TARGET_LAG STRING DEFAULT '1 minute'
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'setup_pipeline_handler'
AS
$$
import json
from snowflake.snowpark import Session

def setup_pipeline_handler(session: Session, file_type: str, chunk_size: int = 1000, chunk_overlap: int = 100, target_lag: str = '1 minute'):
    try:
        if not file_type or not file_type.strip():
            return {"error": "File type cannot be empty"}
        
        file_type = file_type.upper().strip()
        
        # Generate object names
        stage_name = f"{file_type}_STAGE"
        stream_name = f"{file_type}_STREAM"
        task_name = f"{file_type}_PROCESSING_TASK"
        search_service_name = f"{file_type}_SEARCH_SERVICE"
        chunks_table_name = f"{file_type}_CHUNKS"
        
        # Create Chunks Table
        try:
            create_table_sql = f"""
            CREATE OR REPLACE TABLE {chunks_table_name} (
                CHUNK_ID STRING DEFAULT UUID_STRING(),
                FILE_NAME STRING NOT NULL,
                FULL_PATH STRING NOT NULL,
                STAGE_NAME STRING NOT NULL,
                CHUNK_NUMBER INTEGER NOT NULL,
                CHUNK_TEXT STRING NOT NULL,
                CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                PRIMARY KEY (CHUNK_ID)
            )
            """
            session.sql(create_table_sql).collect()
            
        except Exception as e:
            return {"error": f"Failed to create chunks table: {str(e)}"}
        
        # Create Stage
        try:
            create_stage_sql = f"""
            CREATE OR REPLACE STAGE {stage_name}
                DIRECTORY = (ENABLE = TRUE)
                ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
                COMMENT = 'Stage for {file_type} PDF files - Auto processing enabled'
            """
            session.sql(create_stage_sql).collect()
            
        except Exception as e:
            return {"error": f"Failed to create stage: {str(e)}"}
        
        # Create Stream on Stage
        try:
            create_stream_sql = f"""
            CREATE OR REPLACE STREAM {stream_name}
            ON STAGE {stage_name}
            COMMENT = 'Stream to track new files in {file_type} stage'
            """
            session.sql(create_stream_sql).collect()
            
        except Exception as e:
            return {"error": f"Failed to create stream: {str(e)}"}
        
        # Create Search Service
        try:
            create_search_service_sql = f"""
            CREATE OR REPLACE CORTEX SEARCH SERVICE {search_service_name}
            ON CHUNK_TEXT
            ATTRIBUTES FILE_NAME, FULL_PATH, STAGE_NAME, CHUNK_NUMBER
            WAREHOUSE = FROSTLOGIC_WH
            TARGET_LAG = '{target_lag}'
            AS (
                SELECT 
                    CHUNK_TEXT,
                    FILE_NAME,
                    FULL_PATH,
                    STAGE_NAME,
                    CHUNK_NUMBER,
                    CHUNK_ID
                FROM {chunks_table_name}
            )
            """
            session.sql(create_search_service_sql).collect()
            
        except Exception as e:
            return {"error": f"Failed to create search service: {str(e)}"}
        
        # Create Task for Processing
        try:
            task_sql = f"""
            CREATE OR REPLACE TASK {task_name}
                WAREHOUSE = FROSTLOGIC_WH
                COMMENT = 'Auto-process {file_type} files when uploaded'
                WHEN SYSTEM$STREAM_HAS_DATA('{stream_name}')
            AS
            DECLARE
                result VARIANT;
            BEGIN
                CALL PROCESS_FILES_FROM_STREAM(
                    '{file_type}',
                    {chunk_size},
                    {chunk_overlap},
                    '{target_lag}'
                ) INTO result;
                
                IF (result:success::BOOLEAN = TRUE) THEN
                    IF (result:files_processed::INTEGER > 0) THEN
                        SYSTEM$LOG_INFO('File processing completed: ' || result:message::STRING);
                    END IF;
                ELSE
                    SYSTEM$LOG_ERROR('File processing failed: ' || COALESCE(result:error::STRING, 'Unknown error'));
                END IF;
            END;
            """
            
            session.sql(task_sql).collect()
            
        except Exception as e:
            return {"error": f"Failed to create task: {str(e)}"}
        
        # Start the task
        try:
            session.sql(f"ALTER TASK {task_name} RESUME").collect()
        except Exception as e:
            return {"error": f"Failed to start task: {str(e)}"}
        
        return {
            "success": True,
            "file_type": file_type,
            "stage_name": stage_name,
            "stream_name": stream_name,
            "task_name": task_name,
            "cortex_search_service_name": search_service_name,
            "chunks_table_name": chunks_table_name,
            "configuration": {
                "chunk_size": chunk_size,
                "chunk_overlap": chunk_overlap,
                "target_lag": target_lag
            },
            "message": f"Complete file processing pipeline created for {file_type}. Upload PDF files to @{stage_name} and they will be automatically processed.",
            "next_steps": [
                f"Upload PDF files to @{stage_name}",
                f"Files will be automatically chunked within 1 minute",
                f"Data will be stored in {chunks_table_name} table", 
                f"Search service {search_service_name} will refresh automatically",
                f"Monitor task status: SHOW TASKS LIKE '{task_name}'"
            ]
        }
        
    except Exception as e:
        return {"error": f"Pipeline setup failed: {str(e)}"}
$$;

-- Create procedure to check pipeline status
CREATE OR REPLACE PROCEDURE CHECK_PIPELINE_STATUS(
    FILE_TYPE STRING
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'check_status_handler'
AS
$$
import json
from snowflake.snowpark import Session

def check_status_handler(session: Session, file_type: str):
    try:
        if not file_type or not file_type.strip():
            return {"error": "File type cannot be empty"}
        
        file_type = file_type.upper().strip()
        
        # Object names
        stage_name = f"{file_type}_STAGE"
        stream_name = f"{file_type}_STREAM"
        task_name = f"{file_type}_PROCESSING_TASK"
        search_service_name = f"{file_type}_SEARCH_SERVICE"
        chunks_table_name = f"{file_type}_CHUNKS"
        
        status = {
            "file_type": file_type,
            "objects": {}
        }
        
        # Check chunks table
        try:
            table_result = session.sql(f"SHOW TABLES LIKE '{chunks_table_name}'").collect()
            status["objects"]["chunks_table"] = {
                "name": chunks_table_name,
                "exists": len(table_result) > 0,
                "details": table_result[0].as_dict() if table_result else None
            }
        except:
            status["objects"]["chunks_table"] = {"name": chunks_table_name, "exists": False, "error": "Failed to check"}
        
        # Check stage
        try:
            stage_result = session.sql(f"SHOW STAGES LIKE '{stage_name}'").collect()
            status["objects"]["stage"] = {
                "name": stage_name,
                "exists": len(stage_result) > 0,
                "details": stage_result[0].as_dict() if stage_result else None
            }
        except:
            status["objects"]["stage"] = {"name": stage_name, "exists": False, "error": "Failed to check"}
        
        # Check stream
        try:
            stream_result = session.sql(f"SHOW STREAMS LIKE '{stream_name}'").collect()
            status["objects"]["stream"] = {
                "name": stream_name,
                "exists": len(stream_result) > 0,
                "has_data": False
            }
            if stream_result:
                has_data_result = session.sql(f"SELECT SYSTEM$STREAM_HAS_DATA('{stream_name}') as has_data").collect()
                status["objects"]["stream"]["has_data"] = has_data_result[0]["HAS_DATA"] if has_data_result else False
        except:
            status["objects"]["stream"] = {"name": stream_name, "exists": False, "error": "Failed to check"}
        
        # Check task
        try:
            task_result = session.sql(f"SHOW TASKS LIKE '{task_name}'").collect()
            status["objects"]["task"] = {
                "name": task_name,
                "exists": len(task_result) > 0
            }
            if task_result:
                task_info = task_result[0].as_dict()
                status["objects"]["task"]["state"] = task_info.get("STATE")
                status["objects"]["task"]["last_run"] = str(task_info.get("LAST_COMMITTED_ON"))
        except:
            status["objects"]["task"] = {"name": task_name, "exists": False, "error": "Failed to check"}
        
        # Check search service
        try:
            service_result = session.sql(f"SHOW CORTEX SEARCH SERVICES LIKE '{search_service_name}'").collect()
            status["objects"]["search_service"] = {
                "name": search_service_name,
                "exists": len(service_result) > 0
            }
        except:
            status["objects"]["search_service"] = {"name": search_service_name, "exists": False, "error": "Failed to check"}
        
        # Check file chunks
        try:
            chunks_result = session.sql(f"""
                SELECT 
                    COUNT(*) as total_chunks,
                    COUNT(DISTINCT FILE_NAME) as total_files,
                    COUNT(DISTINCT STAGE_NAME) as total_stages,
                    MIN(CREATED_AT) as first_processed,
                    MAX(CREATED_AT) as last_processed
                FROM {chunks_table_name}
            """).collect()
            
            if chunks_result:
                status["data"] = {
                    "total_chunks": chunks_result[0]["TOTAL_CHUNKS"],
                    "total_files": chunks_result[0]["TOTAL_FILES"], 
                    "total_stages": chunks_result[0]["TOTAL_STAGES"],
                    "first_processed": str(chunks_result[0]["FIRST_PROCESSED"]) if chunks_result[0]["FIRST_PROCESSED"] else None,
                    "last_processed": str(chunks_result[0]["LAST_PROCESSED"]) if chunks_result[0]["LAST_PROCESSED"] else None
                }
        except:
            status["data"] = {"error": "Failed to check file chunks"}
        
        return status
        
    except Exception as e:
        return {"error": f"Status check failed: {str(e)}"}
$$;

-- Create procedure to stop/cleanup pipeline
CREATE OR REPLACE PROCEDURE CLEANUP_PIPELINE(
    FILE_TYPE STRING,
    DROP_DATA BOOLEAN DEFAULT FALSE
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'cleanup_handler'
AS
$$
import json
from snowflake.snowpark import Session

def cleanup_handler(session: Session, file_type: str, drop_data: bool = False):
    try:
        if not file_type or not file_type.strip():
            return {"error": "File type cannot be empty"}
        
        file_type = file_type.upper().strip()
        
        # Object names
        stage_name = f"{file_type}_STAGE"
        stream_name = f"{file_type}_STREAM"
        task_name = f"{file_type}_PROCESSING_TASK"
        search_service_name = f"{file_type}_SEARCH_SERVICE"
        chunks_table_name = f"{file_type}_CHUNKS"
        
        cleanup_results = {
            "file_type": file_type,
            "actions": []
        }
        
        # Stop and drop task
        try:
            session.sql(f"ALTER TASK {task_name} SUSPEND").collect()
            cleanup_results["actions"].append(f"Task {task_name} suspended")
            
            session.sql(f"DROP TASK {task_name}").collect()
            cleanup_results["actions"].append(f"Task {task_name} dropped")
        except Exception as e:
            cleanup_results["actions"].append(f"Task cleanup failed: {str(e)}")
        
        # Drop search service
        try:
            session.sql(f"DROP CORTEX SEARCH SERVICE {search_service_name}").collect()
            cleanup_results["actions"].append(f"Search service {search_service_name} dropped")
        except Exception as e:
            cleanup_results["actions"].append(f"Search service cleanup failed: {str(e)}")
        
        # Drop stream
        try:
            session.sql(f"DROP STREAM {stream_name}").collect()
            cleanup_results["actions"].append(f"Stream {stream_name} dropped")
        except Exception as e:
            cleanup_results["actions"].append(f"Stream cleanup failed: {str(e)}")
        
        # Drop stage
        try:
            session.sql(f"DROP STAGE {stage_name}").collect()
            cleanup_results["actions"].append(f"Stage {stage_name} dropped")
        except Exception as e:
            cleanup_results["actions"].append(f"Stage cleanup failed: {str(e)}")
        
        # Drop chunks table and data
        if drop_data:
            try:
                session.sql(f"DROP TABLE {chunks_table_name}").collect()
                cleanup_results["actions"].append(f"Chunks table {chunks_table_name} dropped")
            except Exception as e:
                cleanup_results["actions"].append(f"Table cleanup failed: {str(e)}")
        
        cleanup_results["success"] = True
        cleanup_results["message"] = f"Pipeline cleanup completed for {file_type}"
        
        return cleanup_results
        
    except Exception as e:
        return {"error": f"Cleanup failed: {str(e)}"}
$$;

-- Create procedure to get all file type tables and data
CREATE OR REPLACE PROCEDURE GET_ALL_FILE_TYPE_DATA()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'get_all_data_handler'
AS
$$
import json
from snowflake.snowpark import Session

def get_all_data_handler(session: Session):
    try:
        tables_result = session.sql("SHOW TABLES LIKE '%_CHUNKS'").collect()
        
        if not tables_result:
            return {"message": "No chunk tables found", "tables": []}
        
        all_data = {
            "total_tables": len(tables_result),
            "tables": []
        }
        
        for table_row in tables_result:
            table_name = table_row["name"]
            file_type = table_name.replace("_CHUNKS", "")
            
            try:
                stats_result = session.sql(f"""
                    SELECT 
                        COUNT(*) as total_chunks,
                        COUNT(DISTINCT FILE_NAME) as total_files,
                        COUNT(DISTINCT STAGE_NAME) as total_stages,
                        MIN(CREATED_AT) as first_processed,
                        MAX(CREATED_AT) as last_processed
                    FROM {table_name}
                """).collect()
                
                table_info = {
                    "table_name": table_name,
                    "file_type": file_type,
                    "statistics": {}
                }
                
                if stats_result:
                    stats = stats_result[0]
                    table_info["statistics"] = {
                        "total_chunks": stats["TOTAL_CHUNKS"],
                        "total_files": stats["TOTAL_FILES"],
                        "total_stages": stats["TOTAL_STAGES"],
                        "first_processed": str(stats["FIRST_PROCESSED"]) if stats["FIRST_PROCESSED"] else None,
                        "last_processed": str(stats["LAST_PROCESSED"]) if stats["LAST_PROCESSED"] else None
                    }
                
                all_data["tables"].append(table_info)
                
            except Exception as e:
                all_data["tables"].append({
                    "table_name": table_name,
                    "file_type": file_type,
                    "error": f"Failed to get statistics: {str(e)}"
                })
        
        return all_data
        
    except Exception as e:
        return {"error": f"Failed to get file type data: {str(e)}"}
$$; 

-- ========================================
-- SYNC PROCESSING PROCEDURE
-- ========================================
-- Process files directly from stage that may have been missed by stream processing
-- Useful for handling stream sync issues or manual sync operations

CREATE OR REPLACE PROCEDURE PROCESS_FILES_SYNC(
    FILE_TYPE STRING,
    CHUNK_SIZE INTEGER DEFAULT 1000,
    CHUNK_OVERLAP INTEGER DEFAULT 100,
    FORCE_REPROCESS BOOLEAN DEFAULT FALSE
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'sync_files_handler'
AS
$$
import json
from snowflake.snowpark import Session

def sync_files_handler(session: Session, file_type: str, chunk_size: int = 1000, chunk_overlap: int = 100, force_reprocess: bool = False):
    try:
        if not file_type or not file_type.strip():
            return {"error": "File type cannot be empty"}
        
        file_type = file_type.upper().strip()
        
        # Generate object names from file type
        stage_name = f"{file_type}_STAGE"
        chunks_table_name = f"{file_type}_CHUNKS"
        search_service_name = f"{file_type}_SEARCH_SERVICE"
        
        # Initialize counters
        files_found = 0
        files_processed = 0
        chunks_created = 0
        files_skipped = 0
        
        # Step 1: If force reprocess, truncate chunks table and recreate stream
        if force_reprocess:
            try:
                # Truncate the chunks table
                truncate_sql = f"TRUNCATE TABLE {chunks_table_name}"
                session.sql(truncate_sql).collect()
                
                # Recreate the stream to reset its state
                stream_name = f"{file_type}_STREAM"
                recreate_stream_sql = f"""
                CREATE OR REPLACE STREAM {stream_name}
                ON STAGE {stage_name}
                COMMENT = 'Stream to track new files in {file_type} stage - Recreated during force reprocess'
                """
                session.sql(recreate_stream_sql).collect()
                
            except Exception as e:
                return {"error": f"Failed to truncate chunks table or recreate stream: {str(e)}"}
        
        # Step 2: Get unprocessed files from stage using NOT IN clause
        try:
            # Build the NOT IN condition based on force_reprocess flag
            not_in_condition = ""
            if not force_reprocess:
                not_in_condition = f"""
                AND REGEXP_REPLACE(RELATIVE_PATH, '.*/([^/]+)$', '\\\\1') NOT IN (
                    SELECT DISTINCT FILE_NAME 
                    FROM {chunks_table_name} 
                    WHERE FILE_NAME IS NOT NULL
                )
                """
            
            stage_files_sql = f"""
            SELECT 
                RELATIVE_PATH,
                REGEXP_REPLACE(RELATIVE_PATH, '.*/([^/]+)$', '\\\\1') as file_name,
                SIZE as file_size,
                LAST_MODIFIED
            FROM DIRECTORY(@{stage_name})
            WHERE (UPPER(RELATIVE_PATH) LIKE '%.PDF' OR UPPER(RELATIVE_PATH) LIKE '%.DOCX')
            {not_in_condition}
            ORDER BY RELATIVE_PATH
            """
            
            stage_files_result = session.sql(stage_files_sql).collect()
            files_found = len(stage_files_result)
            
            # Count total files in stage for reporting
            total_files_sql = f"""
            SELECT COUNT(*) as total_files
            FROM DIRECTORY(@{stage_name})
            WHERE (UPPER(RELATIVE_PATH) LIKE '%.PDF' OR UPPER(RELATIVE_PATH) LIKE '%.DOCX')
            """
            total_files_result = session.sql(total_files_sql).collect()
            total_files_in_stage = total_files_result[0]["TOTAL_FILES"] if total_files_result else 0
            
            files_skipped = total_files_in_stage - files_found
            
            if files_found == 0:
                return {
                    "success": True,
                    "message": "No unprocessed PDF or DOCX files found in stage",
                    "file_type": file_type,
                    "stage_name": stage_name,
                    "files_found": total_files_in_stage,
                    "files_processed": 0,
                    "files_skipped": files_skipped,
                    "chunks_created": 0
                }
                
        except Exception as e:
            return {"error": f"Failed to list stage files: {str(e)}"}
        
        # Step 3: Process each unprocessed file (all files in result are unprocessed)
        processing_results = []
        
        for file_row in stage_files_result:
            relative_path = file_row["RELATIVE_PATH"]
            file_name = file_row["FILE_NAME"]
            
            try:
                # Step 3a: Parse and chunk the document
                parse_and_chunk_sql = f"""
                WITH dummy AS (SELECT 1 as dummy_col)
                SELECT 
                    '{file_name.replace("'", "''")}' as file_name,
                    '{relative_path.replace("'", "''")}' as full_path,
                    '{stage_name}' as stage_name,
                    c.index + 1 as chunk_number,
                    c.value::STRING as chunk_text
                FROM 
                    dummy,
                    LATERAL FLATTEN(
                        SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER(
                            SNOWFLAKE.CORTEX.PARSE_DOCUMENT(
                                '@{stage_name}',
                                '{relative_path.replace("'", "''")}',
                                {{'mode': 'LAYOUT'}}
                            ):content::STRING,
                            'markdown',
                            {chunk_size},
                            {chunk_overlap}
                        )
                    ) c
                WHERE LENGTH(TRIM(c.value::STRING)) > 0
                """
                
                # Execute the parse and get results
                chunk_results = session.sql(parse_and_chunk_sql).collect()
                
                if chunk_results:
                    # Step 3b: Insert chunks into table
                    file_chunks = 0
                    for chunk_row in chunk_results:
                        insert_sql = f"""
                        INSERT INTO {chunks_table_name} (FILE_NAME, FULL_PATH, STAGE_NAME, CHUNK_NUMBER, CHUNK_TEXT)
                        VALUES (
                            '{chunk_row["FILE_NAME"].replace("'", "''")}',
                            '{chunk_row["FULL_PATH"].replace("'", "''")}',
                            '{chunk_row["STAGE_NAME"]}',
                            {chunk_row["CHUNK_NUMBER"]},
                            '{chunk_row["CHUNK_TEXT"].replace("'", "''")}'
                        )
                        """
                        session.sql(insert_sql).collect()
                        file_chunks += 1
                    
                    files_processed += 1
                    chunks_created += file_chunks
                    
                    processing_results.append({
                        "file_name": file_name,
                        "status": "processed",
                        "chunks_created": file_chunks,
                        "file_size": file_row["FILE_SIZE"]
                    })
                    
                else:
                    processing_results.append({
                        "file_name": file_name,
                        "status": "failed",
                        "reason": "no_chunks_extracted"
                    })
                    
            except Exception as e:
                processing_results.append({
                    "file_name": file_name,
                    "status": "error",
                    "error": str(e)
                })
        
        # Step 4: Refresh search service if files were processed
        search_service_refreshed = False
        if files_processed > 0:
            try:
                refresh_sql = f"ALTER CORTEX SEARCH SERVICE {search_service_name} REFRESH"
                session.sql(refresh_sql).collect()
                search_service_refreshed = True
            except Exception as e:
                # Search service refresh failure is not critical
                pass
        
        return {
            "success": True,
            "file_type": file_type,
            "stage_name": stage_name,
            "chunks_table_name": chunks_table_name,
            "search_service_name": search_service_name,
            "files_found": total_files_in_stage,
            "files_processed": files_processed,
            "files_skipped": files_skipped,
            "chunks_created": chunks_created,
            "force_reprocess": force_reprocess,
            "search_service_refreshed": search_service_refreshed,
            "processing_details": processing_results,
            "message": f"Sync completed: {files_processed} files processed, {files_skipped} skipped, {chunks_created} chunks created"
        }
        
    except Exception as e:
        return {"error": f"Sync processing failed: {str(e)}"}
$$;
