import streamlit as st
import os
import tempfile
from datetime import datetime
from snowflake.snowpark.context import get_active_session
from typing import Dict, List, Any, Optional

st.set_page_config(
    page_title='File Upload - Frostlogic',
    page_icon='📤',
    layout='wide'
)

class FileUploadManager:
    def __init__(self):
        self.session = get_active_session()
    
    def get_available_file_types(self) -> List[Dict[str, Any]]:
        """Get available file type configurations"""
        try:
            result = self.session.sql("CALL FILE_TYPE_CONFIGS_GET()").collect()
            return [row.as_dict() for row in result]
        except Exception as e:
            st.error(f"Failed to load file type configurations: {str(e)}")
            return []
    
    def check_stage_exists(self, file_type: str) -> bool:
        """Check if stage exists for the file type"""
        try:
            stage_name = f"{file_type}_STAGE"
            result = self.session.sql(f"SHOW STAGES LIKE '{stage_name}'").collect()
            return len(result) > 0
        except Exception as e:
            st.error(f"Error checking stage: {str(e)}")
            return False
    
    def upload_file_to_stage(self, file_type: str, uploaded_file, file_content: bytes) -> Dict[str, Any]:
        """Upload file to Snowflake stage"""
        try:
            stage_name = f"@{file_type}_STAGE"
            
            # Create temporary file
            with tempfile.NamedTemporaryFile(delete=False, suffix=f"_{uploaded_file.name}") as tmp_file:
                tmp_file.write(file_content)
                tmp_file_path = tmp_file.name
            
            try:
                # Upload file to stage using PUT command
                put_sql = f"PUT 'file://{tmp_file_path}' {stage_name} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
                result = self.session.sql(put_sql).collect()
                
                if result and len(result) > 0:
                    upload_result = result[0]
                    return {
                        "success": True,
                        "message": f"File uploaded successfully to {stage_name}",
                        "file_name": uploaded_file.name,
                        "size_bytes": len(file_content),
                        "stage": stage_name,
                        "status": upload_result.get("status", "UPLOADED") if hasattr(upload_result, 'get') else "UPLOADED"
                    }
                else:
                    return {
                        "success": False,
                        "error": "No result returned from upload command"
                    }
                    
            finally:
                # Clean up temporary file
                try:
                    os.unlink(tmp_file_path)
                except:
                    pass
                    
        except Exception as e:
            return {
                "success": False,
                "error": f"Upload failed: {str(e)}"
            }
    
    def list_stage_files(self, file_type: str) -> List[Dict[str, Any]]:
        """List files in the stage"""
        try:
            stage_name = f"@{file_type}_STAGE"
            result = self.session.sql(f"LIST {stage_name}").collect()
            
            files = []
            for row in result:
                row_dict = row.as_dict()
                files.append({
                    "name": row_dict.get("name", "Unknown"),
                    "size": row_dict.get("size", 0),
                    "last_modified": row_dict.get("last_modified", "Unknown"),
                    "md5": row_dict.get("md5", "Unknown")
                })
            
            return files
        except Exception as e:
            st.error(f"Failed to list stage files: {str(e)}")
            return []
    
    def check_pipeline_status(self, file_type: str) -> Dict[str, Any]:
        """Check pipeline status for file type"""
        try:
            result = self.session.sql(f"CALL CHECK_PIPELINE_STATUS('{file_type}')").collect()
            if result:
                response = result[0]["CHECK_PIPELINE_STATUS"]
                if isinstance(response, str):
                    import json
                    response = json.loads(response)
                return response
            return {"error": "No response"}
        except Exception as e:
            return {"error": str(e)}
    
    def get_processed_files_status(self, file_type: str) -> List[Dict[str, Any]]:
        """Get status of processed files with proper stage paths"""
        try:
            result = self.session.sql(f"CALL FILES_GET_BY_TYPE('{file_type}')").collect()
            files = []
            for row in result:
                file_dict = row.as_dict()
                # Construct full stage path for stored procedure calls
                stage_name = file_dict.get('STAGE_NAME', '')
                full_path = file_dict.get('FULL_PATH', '')
                file_dict['STAGE_PATH'] = f"@{stage_name}/{full_path}" if full_path else f"@{stage_name}"
                files.append(file_dict)
            return files
        except Exception as e:
            st.error(f"Failed to get processed files: {str(e)}")
            return []

def render_file_upload_interface(upload_mgr: FileUploadManager):
    """Render the main file upload interface"""
    st.subheader("📁 File Management")
    st.markdown("Upload PDF and DOCX files to document stages for processing")
    
    # Add warning about upload not being supported
    st.warning(
        "⚠️ **File Upload Not Currently Supported** - Please use Snowsight or Snow CLI to upload files to stages",
        icon="⚠️"
    )
    
    st.markdown("---")
    
    # Get available file types
    file_types = upload_mgr.get_available_file_types()
    
    if not file_types:
        st.error("No document types configured. Please configure document types first in the Configuration page.")
        return
    
    # File type selection
    selected_file_type = st.selectbox(
        "Select Document Type:",
        options=[f['FILE_TYPE'] for f in file_types],
        help="Choose the document type for uploading files"
    )
    
    if selected_file_type:
        # Get configuration details for selected type
        selected_config = next((ft for ft in file_types if ft['FILE_TYPE'] == selected_file_type), None)
        
        if selected_config:
            # Display configuration info
            col1, col2 = st.columns(2)
            
            with col1:
                st.info(f"""
                **📋 {selected_file_type} Configuration**
                
                **Description:** {selected_config['FILE_DESCRIPTION']}
                **Chunk Size:** {selected_config['CHUNK_SIZE']}
                **Chunk Overlap:** {selected_config['CHUNK_OVERLAP']}
                **Processing Lag:** {selected_config['TARGET_LAG']}
                """)
            
            with col2:
                # Check if stage exists and pipeline is set up
                stage_exists = upload_mgr.check_stage_exists(selected_file_type)
                
                if stage_exists:
                    st.success(f"✅ Stage `{selected_file_type}_STAGE` is ready for uploads")
                    
                    # Check pipeline status
                    pipeline_status = upload_mgr.check_pipeline_status(selected_file_type)
                    if "error" not in pipeline_status:
                        objects = pipeline_status.get("objects", {})
                        
                        pipeline_ready = all([
                            objects.get("stage", {}).get("exists", False),
                            objects.get("stream", {}).get("exists", False),
                            objects.get("task", {}).get("exists", False),
                            objects.get("search_service", {}).get("exists", False)
                        ])
                        
                        if pipeline_ready:
                            st.success("🚀 Processing pipeline is active and ready")
                        else:
                            st.warning("⚠️ Processing pipeline not fully configured")
                            st.write("Missing components:")
                            for obj_name, obj_info in objects.items():
                                if not obj_info.get("exists", False):
                                    st.write(f"❌ {obj_name.replace('_', ' ').title()}")
                    else:
                        st.error(f"Pipeline status check failed: {pipeline_status['error']}")
                else:
                    st.error(f"❌ Stage `{selected_file_type}_STAGE` not found")
                    st.write("Please set up the processing pipeline first in the Configuration page.")
                    return
            
            st.markdown("---")
            
            # File upload section
            st.markdown("### 📁 File Management")
            
            # File uploader
            uploaded_files = st.file_uploader(
                "Choose PDF or DOCX files:",
                type=['pdf', 'docx'],
                accept_multiple_files=True,
                help="Select one or more PDF or DOCX files to upload"
            )
            
            if uploaded_files:
                st.write(f"**Selected Files:** {len(uploaded_files)}")
                
                # Display file info
                total_size = 0
                for file in uploaded_files:
                    file_size = len(file.getvalue())
                    total_size += file_size
                    size_mb = file_size / (1024 * 1024)
                    st.write(f"📄 {file.name} ({size_mb:.2f} MB)")
                
                total_size_mb = total_size / (1024 * 1024)
                st.write(f"**Total Size:** {total_size_mb:.2f} MB")
                
                # Upload button
                col1, col2 = st.columns(2)
                
                with col1:
                    if st.button("🚀 Manage Files", type="primary", use_container_width=True):
                        upload_results = []
                        
                        # Create progress bar
                        progress_bar = st.progress(0)
                        status_text = st.empty()
                        
                        for i, file in enumerate(uploaded_files):
                            status_text.text(f"Uploading {file.name}...")
                            progress_bar.progress((i) / len(uploaded_files))
                            
                            # Upload file
                            file_content = file.getvalue()
                            result = upload_mgr.upload_file_to_stage(selected_file_type, file, file_content)
                            upload_results.append(result)
                        
                        # Complete progress
                        progress_bar.progress(1.0)
                        status_text.text("Upload completed!")
                        
                        # Display results
                        st.markdown("---")
                        st.subheader("📊 Upload Results")
                        
                        successful_uploads = 0
                        failed_uploads = 0
                        
                        for result in upload_results:
                            if result.get("success"):
                                successful_uploads += 1
                                st.success(f"✅ {result['file_name']}: {result['message']}")
                            else:
                                failed_uploads += 1
                                st.error(f"❌ {result.get('file_name', 'Unknown')}: {result.get('error', 'Unknown error')}")
                        
                        # Summary
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Total Files", len(uploaded_files))
                        with col2:
                            st.metric("Successful", successful_uploads)
                        with col3:
                            st.metric("Failed", failed_uploads)
                        
                        if successful_uploads > 0:
                            st.info(f"💡 Files will be automatically processed within {selected_config['TARGET_LAG']}. Check the Processing Status section below for updates.")
                
                with col2:
                    if st.button("🔄 Refresh Page", use_container_width=True):
                        st.rerun()

def render_stage_file_browser(upload_mgr: FileUploadManager):
    """Render stage file browser and management"""
    st.subheader("📂 Stage File Browser")
    st.markdown("View and manage files in document stages")
    
    # Get available file types
    file_types = upload_mgr.get_available_file_types()
    
    if not file_types:
        st.info("No document types configured.")
        return
    
    # File type selection for browsing
    browse_file_type = st.selectbox(
        "Browse Stage:",
        options=[f['FILE_TYPE'] for f in file_types],
        key="browse_file_type",
        help="Select a document type to browse its stage files"
    )
    
    if browse_file_type:
        # Check if stage exists
        stage_exists = upload_mgr.check_stage_exists(browse_file_type)
        
        if stage_exists:
            # Get files in stage
            stage_files = upload_mgr.list_stage_files(browse_file_type)
            
            if stage_files:
                st.write(f"**Files in `{browse_file_type}_STAGE`:** {len(stage_files)}")
                
                # Display files in a table
                file_data = []
                for file in stage_files:
                    # Clean up file name (remove stage prefix and path)
                    clean_name = file['name'].split('/')[-1] if '/' in file['name'] else file['name']
                    
                    # Format file size
                    size_bytes = file.get('size', 0)
                    if size_bytes < 1024:
                        size_str = f"{size_bytes} B"
                    elif size_bytes < 1024 * 1024:
                        size_str = f"{size_bytes / 1024:.1f} KB"
                    else:
                        size_str = f"{size_bytes / (1024 * 1024):.1f} MB"
                    
                    file_data.append({
                        "File Name": clean_name,
                        "Size": size_str,
                        "Last Modified": file.get('last_modified', 'Unknown'),
                        "MD5": file.get('md5', 'Unknown')[:16] + "..." if file.get('md5') else "Unknown"
                    })
                
                if file_data:
                    import pandas as pd
                    df = pd.DataFrame(file_data)
                    st.dataframe(df, use_container_width=True, hide_index=True)
                
            else:
                st.info(f"No files found in `{browse_file_type}_STAGE`")
        else:
            st.error(f"Stage `{browse_file_type}_STAGE` not found. Please set up the pipeline first.")

def render_processing_status(upload_mgr: FileUploadManager):
    """Render processing status for uploaded files"""
    st.subheader("⚙️ Processing Status")
    st.markdown("Monitor document processing and search service status")
    
    # Get available file types
    file_types = upload_mgr.get_available_file_types()
    
    if not file_types:
        st.info("No document types configured.")
        return
    
    # Status overview for all file types
    st.markdown("### 📊 Processing Overview")
    
    status_data = []
    for file_type_config in file_types:
        file_type = file_type_config['FILE_TYPE']
        
        # Get pipeline status
        pipeline_status = upload_mgr.check_pipeline_status(file_type)
        
        if "error" not in pipeline_status:
            objects = pipeline_status.get("objects", {})
            data_info = pipeline_status.get("data", {})
            
            # Get stream data status
            stream_has_data = objects.get("stream", {}).get("has_data", False)
            task_state = objects.get("task", {}).get("state", "Unknown")
            
            status_data.append({
                "Document Type": file_type,
                "Pipeline Status": "✅ Active" if all(obj.get("exists", False) for obj in objects.values()) else "❌ Incomplete",
                "Task State": task_state,
                "Pending Files": "✅ Yes" if stream_has_data else "❌ No",
                "Total Files": data_info.get("total_files", 0),
                "Total Chunks": data_info.get("total_chunks", 0),
                "Last Processed": data_info.get("last_processed", "Never")
            })
        else:
            status_data.append({
                "Document Type": file_type,
                "Pipeline Status": "❌ Error",
                "Task State": "Unknown",
                "Pending Files": "Unknown",
                "Total Files": 0,
                "Total Chunks": 0,
                "Last Processed": "Never"
            })
    
    if status_data:
        import pandas as pd
        df = pd.DataFrame(status_data)
        st.dataframe(df, use_container_width=True, hide_index=True)
    
    st.markdown("---")
    
    # Detailed status for specific file type
    st.markdown("### 🔍 Detailed Processing Status")
    
    status_file_type = st.selectbox(
        "Select Document Type for Detailed Status:",
        options=[f['FILE_TYPE'] for f in file_types],
        key="status_file_type",
        help="Choose a document type to view detailed processing status"
    )
    
    if status_file_type:
        col1, col2 = st.columns(2)
        
        with col1:
            # Pipeline component status
            st.markdown("**🚧 Pipeline Components**")
            pipeline_status = upload_mgr.check_pipeline_status(status_file_type)
            
            if "error" not in pipeline_status:
                objects = pipeline_status.get("objects", {})
                
                for obj_name, obj_info in objects.items():
                    status_icon = "✅" if obj_info.get("exists") else "❌"
                    obj_display_name = obj_name.replace('_', ' ').title()
                    st.write(f"{status_icon} **{obj_display_name}**")
                    
                    # Show additional details
                    if obj_name == "task" and obj_info.get("exists"):
                        st.write(f"   • State: {obj_info.get('state', 'Unknown')}")
                        st.write(f"   • Last Run: {obj_info.get('last_run', 'Never')}")
                    elif obj_name == "stream" and obj_info.get("exists"):
                        has_data_text = "Yes" if obj_info.get("has_data") else "No"
                        st.write(f"   • Has Pending Data: {has_data_text}")
            else:
                st.error(f"Status check failed: {pipeline_status['error']}")
        
        with col2:
            # Processing statistics
            st.markdown("**📈 Processing Statistics**")
            processed_files = upload_mgr.get_processed_files_status(status_file_type)
            
            if processed_files:
                st.write(f"**Processed Files:** {len(processed_files)}")
                
                # Show recent files
                if len(processed_files) > 0:
                    st.write("**Recent Files:**")
                    for file_info in processed_files[:5]:  # Show top 5 recent files
                        file_name = file_info.get('FILE_NAME', 'Unknown')
                        chunk_count = file_info.get('CHUNK_COUNT', 0)
                        last_processed = file_info.get('LAST_PROCESSED', 'Unknown')
                        
                        st.write(f"📄 {file_name}")
                        st.write(f"   • Chunks: {chunk_count}")
                        st.write(f"   • Processed: {last_processed}")
                
                if len(processed_files) > 5:
                    st.write(f"... and {len(processed_files) - 5} more files")
            else:
                st.info("No processed files found")
        
        # Refresh button
        if st.button("🔄 Refresh Status", key=f"refresh_{status_file_type}"):
            st.rerun()

def main():
    """Main file upload page"""
    st.title("📁 File Management")
    st.markdown("Upload documents and monitor processing status")
    st.markdown("---")
    
    # Initialize upload manager
    upload_mgr = FileUploadManager()
    
    # Create tabs
    tab1, tab2, tab3 = st.tabs(["📁 File Management", "📂 Browse Stages", "⚙️ Processing Status"])
    
    with tab1:
        render_file_upload_interface(upload_mgr)
    
    with tab2:
        render_stage_file_browser(upload_mgr)
    
    with tab3:
        render_processing_status(upload_mgr)

if __name__ == "__main__":
    main() 