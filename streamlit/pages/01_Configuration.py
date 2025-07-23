import streamlit as st
import json
import pandas as pd
from datetime import datetime
from snowflake.snowpark.context import get_active_session
from typing import Dict, List, Any, Optional

st.set_page_config(
    page_title='Configuration - Frostlogic',
    page_icon='⚙️',
    layout='wide'
)

class ConfigurationManager:
    def __init__(self):
        self.session = get_active_session()
    
    # ========================================
    # MODEL MANAGEMENT METHODS
    # ========================================
    
    def get_available_models(self) -> Dict[str, Any]:
        """Get available AI models from GET_AVAILABLE_MODELS procedure"""
        try:
            result = self.session.sql("CALL GET_AVAILABLE_MODELS()").collect()
            if result:
                models_data = result[0]["GET_AVAILABLE_MODELS"]
                if isinstance(models_data, str):
                    models_data = json.loads(models_data)
                return models_data
            return {
                "supported_models": ["claude-4-sonnet"],
                "default_model": "claude-4-sonnet",
                "recommended_models": {"quality": "claude-4-sonnet"}
            }
        except Exception as e:
            st.error(f"Failed to load available models: {str(e)}")
            return {
                "supported_models": ["claude-4-sonnet"],
                "default_model": "claude-4-sonnet", 
                "recommended_models": {"quality": "claude-4-sonnet"}
            }
    
    # ========================================
    # FILE TYPE CONFIG METHODS
    # ========================================
    
    def get_file_type_configs(self) -> List[Dict[str, Any]]:
        """Get all file type configurations"""
        try:
            result = self.session.sql("CALL FILE_TYPE_CONFIGS_GET()").collect()
            return [row.as_dict() for row in result]
        except Exception as e:
            st.error(f"Failed to load file type configurations: {str(e)}")
            return []
    
    def create_file_type_config(self, file_type: str, file_description: str, chunk_size: int, chunk_overlap: int, target_lag: str) -> Dict[str, Any]:
        """Create new file type configuration"""
        try:
            sql = f"""
            CALL FILE_TYPE_CONFIGS_CREATE(
                '{file_type}',
                '{file_description.replace("'", "''")}',
                {chunk_size},
                {chunk_overlap},
                '{target_lag}'
            )
            """
            result = self.session.sql(sql).collect()
            if result:
                response = result[0]["FILE_TYPE_CONFIGS_CREATE"]
                if isinstance(response, str):
                    response = json.loads(response)
                return response
            return {"success": False, "error": "No response"}
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def update_file_type_config(self, file_type: str, file_description: str, chunk_size: int, chunk_overlap: int, target_lag: str) -> Dict[str, Any]:
        """Update file type configuration"""
        try:
            sql = f"""
            CALL FILE_TYPE_CONFIGS_UPDATE(
                '{file_type}',
                '{file_description.replace("'", "''")}',
                {chunk_size},
                {chunk_overlap},
                '{target_lag}'
            )
            """
            result = self.session.sql(sql).collect()
            if result:
                response = result[0]["FILE_TYPE_CONFIGS_UPDATE"]
                if isinstance(response, str):
                    response = json.loads(response)
                return response
            return {"success": False, "error": "No response"}
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def delete_file_type_config(self, file_type: str, drop_data: bool = False) -> Dict[str, Any]:
        """Delete file type configuration and cleanup pipeline"""
        try:
            sql = f"CALL FILE_TYPE_CONFIGS_DELETE('{file_type}', {str(drop_data).lower()})"
            result = self.session.sql(sql).collect()
            if result:
                response = result[0]["FILE_TYPE_CONFIGS_DELETE"]
                if isinstance(response, str):
                    response = json.loads(response)
                return response
            return {"success": False, "error": "No response"}
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def setup_file_processing_pipeline(self, file_type: str, chunk_size: int, chunk_overlap: int, target_lag: str) -> Dict[str, Any]:
        """Setup complete processing pipeline for file type"""
        try:
            sql = f"""
            CALL SETUP_FILE_PROCESSING_PIPELINE(
                '{file_type}',
                {chunk_size},
                {chunk_overlap},
                '{target_lag}'
            )
            """
            result = self.session.sql(sql).collect()
            if result:
                response = result[0]["SETUP_FILE_PROCESSING_PIPELINE"]
                if isinstance(response, str):
                    response = json.loads(response)
                return response
            return {"success": False, "error": "No response"}
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def check_pipeline_status(self, file_type: str) -> Dict[str, Any]:
        """Check pipeline status for file type"""
        try:
            sql = f"CALL CHECK_PIPELINE_STATUS('{file_type}')"
            result = self.session.sql(sql).collect()
            if result:
                response = result[0]["CHECK_PIPELINE_STATUS"]
                if isinstance(response, str):
                    response = json.loads(response)
                return response
            return {"error": "No response"}
        except Exception as e:
            return {"error": str(e)}
    
    def sync_files(self, file_type: str, chunk_size: int = 1000, chunk_overlap: int = 100, force_reprocess: bool = False) -> Dict[str, Any]:
        """Manually sync files from stage to chunks table"""
        try:
            sql = f"""
            CALL PROCESS_FILES_SYNC(
                '{file_type}',
                {chunk_size},
                {chunk_overlap},
                {str(force_reprocess).lower()}
            )
            """
            result = self.session.sql(sql).collect()
            if result:
                response = result[0]["PROCESS_FILES_SYNC"]
                if isinstance(response, str):
                    response = json.loads(response)
                return response
            return {"success": False, "error": "No response"}
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    # ========================================
    # PROCESSING CONFIG METHODS
    # ========================================
    
    def get_processing_configs(self) -> List[Dict[str, Any]]:
        """Get all processing configurations"""
        try:
            result = self.session.sql("CALL PROCESSING_CONFIGS_GET()").collect()
            configs = []
            for row in result:
                config_data = row.as_dict()
                # Parse CONFIG_JSON if it's a string
                if isinstance(config_data['CONFIG_JSON'], str):
                    config_data['CONFIG_JSON'] = json.loads(config_data['CONFIG_JSON'])
                configs.append(config_data)
            return configs
        except Exception as e:
            st.error(f"Failed to load processing configurations: {str(e)}")
            return []
    
    def create_processing_config(self, config_name: str, processing_type: str, config_model: str, config_json: Dict) -> Dict[str, Any]:
        """Create new processing configuration"""
        try:
            config_json_str = json.dumps(config_json).replace("'", "''")
            sql = f"""
            CALL PROCESSING_CONFIGS_CREATE(
                '{config_name}',
                '{processing_type}',
                '{config_json_str}',
                '{config_model}'
            )
            """
            result = self.session.sql(sql).collect()
            if result:
                response = result[0]["PROCESSING_CONFIGS_CREATE"]
                if isinstance(response, str):
                    response = json.loads(response)
                return response
            return {"success": False, "error": "No response"}
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def update_processing_config(self, config_name: str, processing_type: str, config_model: str, config_json: Dict) -> Dict[str, Any]:
        """Update processing configuration"""
        try:
            config_json_str = json.dumps(config_json).replace("'", "''")
            sql = f"""
            CALL PROCESSING_CONFIGS_UPDATE(
                '{config_name}',
                '{processing_type}',
                '{config_json_str}',
                '{config_model}'
            )
            """
            result = self.session.sql(sql).collect()
            if result:
                response = result[0]["PROCESSING_CONFIGS_UPDATE"]
                if isinstance(response, str):
                    response = json.loads(response)
                return response
            return {"success": False, "error": "No response"}
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def delete_processing_config(self, config_name: str) -> Dict[str, Any]:
        """Delete processing configuration"""
        try:
            sql = f"CALL PROCESSING_CONFIGS_DELETE('{config_name}')"
            result = self.session.sql(sql).collect()
            if result:
                response = result[0]["PROCESSING_CONFIGS_DELETE"]
                if isinstance(response, str):
                    response = json.loads(response)
                return response
            return {"success": False, "error": "No response"}
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def validate_processing_config(self, config_json: Dict) -> Dict[str, Any]:
        """Validate processing configuration JSON"""
        try:
            config_json_str = json.dumps(config_json).replace("'", "''")
            sql = f"CALL PROCESSING_CONFIGS_VALIDATE('{config_json_str}')"
            result = self.session.sql(sql).collect()
            if result:
                response = result[0]["PROCESSING_CONFIGS_VALIDATE"]
                if isinstance(response, str):
                    response = json.loads(response)
                return response
            return {"is_valid": False, "errors": ["No response"]}
        except Exception as e:
            return {"is_valid": False, "errors": [str(e)]}

def convert_string_to_appropriate_type(value: str):
    """Convert string value to appropriate type (int, bool, or keep as string)"""
    if not isinstance(value, str):
        return value
    
    # Try to convert to integer
    try:
        # Check if it looks like an integer
        if value.isdigit() or (value.startswith('-') and value[1:].isdigit()):
            return int(value)
    except:
        pass
    
    # Try to convert to float
    try:
        if '.' in value and value.replace('.', '').replace('-', '').isdigit():
            return float(value)
    except:
        pass
    
    # Try to convert to boolean
    if value.lower() in ('true', 'false'):
        return value.lower() == 'true'
    
    # Return as string if no conversion worked
    return value

def render_file_type_management(config_mgr: ConfigurationManager):
    """Render the file type management tab"""
    st.subheader("📁 Document Type Management")
    st.markdown("Configure document types and their processing parameters")
    
    # Load existing configurations
    file_configs = config_mgr.get_file_type_configs()
    
    # Display existing configurations
    if file_configs:
        st.markdown("### Existing Document Types")
        
        # Convert to DataFrame for display
        df = pd.DataFrame(file_configs)
        
        # Display configurations
        for i, config in enumerate(file_configs):
            with st.expander(f"📄 {config['FILE_TYPE']} - {config['FILE_DESCRIPTION']}", expanded=False):
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write(f"**File Type:** {config['FILE_TYPE']}")
                    st.write(f"**Description:** {config['FILE_DESCRIPTION']}")
                    st.write(f"**Chunk Size:** {config['CHUNK_SIZE']}")
                
                with col2:
                    st.write(f"**Chunk Overlap:** {config['CHUNK_OVERLAP']}")
                    st.write(f"**Target Lag:** {config['TARGET_LAG']}")
                
                # Action buttons
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    if st.button(f"✏️ Edit", key=f"edit_{config['FILE_TYPE']}"):
                        st.session_state[f"editing_{config['FILE_TYPE']}"] = True
                        st.rerun()
                
                with col2:
                    if st.button(f"🚀 Setup Pipeline", key=f"setup_{config['FILE_TYPE']}"):
                        with st.spinner(f"Setting up pipeline for {config['FILE_TYPE']}..."):
                            result = config_mgr.setup_file_processing_pipeline(
                                config['FILE_TYPE'],
                                config['CHUNK_SIZE'],
                                config['CHUNK_OVERLAP'],
                                config['TARGET_LAG']
                            )
                            if result.get("success"):
                                st.success(f"✅ Pipeline setup completed for {config['FILE_TYPE']}")
                                st.write("**Next Steps:**")
                                for step in result.get("next_steps", []):
                                    st.write(f"• {step}")
                            else:
                                st.error(f"❌ Pipeline setup failed: {result.get('error', 'Unknown error')}")
                
                with col3:
                    if st.button(f"📊 Check Status", key=f"status_{config['FILE_TYPE']}"):
                        status = config_mgr.check_pipeline_status(config['FILE_TYPE'])
                        if "error" not in status:
                            st.write("**Pipeline Status:**")
                            for obj_name, obj_info in status.get("objects", {}).items():
                                status_icon = "✅" if obj_info.get("exists") else "❌"
                                st.write(f"{status_icon} {obj_name.replace('_', ' ').title()}: {obj_info.get('name')}")
                            
                            if "data" in status:
                                data_info = status["data"]
                                st.write("**Data Statistics:**")
                                st.write(f"• Total Files: {data_info.get('total_files', 0)}")
                                st.write(f"• Total Chunks: {data_info.get('total_chunks', 0)}")
                        else:
                            st.error(f"❌ Status check failed: {status['error']}")
                
                with col4:
                    if st.button(f"🗑️ Delete", key=f"delete_{config['FILE_TYPE']}", type="secondary"):
                        st.session_state[f"confirm_delete_{config['FILE_TYPE']}"] = True
                        st.rerun()
                
                # Edit form
                if st.session_state.get(f"editing_{config['FILE_TYPE']}", False):
                    st.markdown("---")
                    st.markdown("**Edit Configuration:**")
                    
                    edit_col1, edit_col2 = st.columns(2)
                    
                    with edit_col1:
                        new_description = st.text_input(
                            "Description:",
                            value=config['FILE_DESCRIPTION'],
                            key=f"edit_desc_{config['FILE_TYPE']}"
                        )
                        new_chunk_size = st.number_input(
                            "Chunk Size:",
                            min_value=100,
                            max_value=5000,
                            value=config['CHUNK_SIZE'],
                            step=100,
                            key=f"edit_chunk_{config['FILE_TYPE']}"
                        )
                    
                    with edit_col2:
                        new_chunk_overlap = st.number_input(
                            "Chunk Overlap:",
                            min_value=0,
                            max_value=1000,
                            value=config['CHUNK_OVERLAP'],
                            step=10,
                            key=f"edit_overlap_{config['FILE_TYPE']}"
                        )
                        new_target_lag = st.selectbox(
                            "Target Lag:",
                            options=["1 minute", "2 minutes", "5 minutes", "10 minutes"],
                            index=["1 minute", "2 minutes", "5 minutes", "10 minutes"].index(config['TARGET_LAG']) if config['TARGET_LAG'] in ["1 minute", "2 minutes", "5 minutes", "10 minutes"] else 0,
                            key=f"edit_lag_{config['FILE_TYPE']}"
                        )
                    
                    edit_col1, edit_col2 = st.columns(2)
                    
                    with edit_col1:
                        if st.button(f"💾 Save Changes", key=f"save_{config['FILE_TYPE']}"):
                            result = config_mgr.update_file_type_config(
                                config['FILE_TYPE'],
                                new_description,
                                new_chunk_size,
                                new_chunk_overlap,
                                new_target_lag
                            )
                            if result.get("success"):
                                st.success("✅ Configuration updated successfully!")
                                st.session_state[f"editing_{config['FILE_TYPE']}"] = False
                                st.rerun()
                            else:
                                st.error(f"❌ Update failed: {result.get('error', 'Unknown error')}")
                    
                    with edit_col2:
                        if st.button(f"❌ Cancel", key=f"cancel_{config['FILE_TYPE']}"):
                            st.session_state[f"editing_{config['FILE_TYPE']}"] = False
                            st.rerun()
                
                # Delete confirmation
                if st.session_state.get(f"confirm_delete_{config['FILE_TYPE']}", False):
                    st.markdown("---")
                    st.warning(f"⚠️ **Confirm Deletion of {config['FILE_TYPE']}**")
                    st.write("This will remove the configuration and cleanup the processing pipeline.")
                    
                    drop_data = st.checkbox(
                        "Also delete all processed data (chunks, files)",
                        key=f"drop_data_{config['FILE_TYPE']}",
                        help="WARNING: This will permanently delete all processed documents and chunks"
                    )
                    
                    del_col1, del_col2 = st.columns(2)
                    
                    with del_col1:
                        if st.button(f"🗑️ Confirm Delete", key=f"confirm_{config['FILE_TYPE']}", type="primary"):
                            with st.spinner(f"Deleting {config['FILE_TYPE']}..."):
                                result = config_mgr.delete_file_type_config(config['FILE_TYPE'], drop_data)
                                if result.get("success"):
                                    st.success(f"✅ {config['FILE_TYPE']} deleted successfully!")
                                    st.write("**Actions performed:**")
                                    for action in result.get("actions", []):
                                        st.write(f"• {action}")
                                    st.session_state[f"confirm_delete_{config['FILE_TYPE']}"] = False
                                    st.rerun()
                                else:
                                    st.error(f"❌ Deletion failed: {result.get('error', 'Unknown error')}")
                    
                    with del_col2:
                        if st.button(f"❌ Cancel Delete", key=f"cancel_delete_{config['FILE_TYPE']}"):
                            st.session_state[f"confirm_delete_{config['FILE_TYPE']}"] = False
                            st.rerun()
    
    st.markdown("---")
    
    # Add new file type configuration
    st.markdown("### ➕ Add New Document Type")
    
    with st.expander("Create New Document Type", expanded=False):
        col1, col2 = st.columns(2)
        
        with col1:
            new_file_type = st.text_input(
                "File Type:",
                placeholder="e.g., CONTRACT, INVOICE, REPORT",
                help="Use uppercase letters, underscores allowed"
            )
            new_description = st.text_input(
                "Description:",
                placeholder="e.g., Contract documents for legal review"
            )
            new_chunk_size = st.number_input(
                "Chunk Size:",
                min_value=100,
                max_value=5000,
                value=1000,
                step=100,
                help="Size of text chunks for processing"
            )
        
        with col2:
            new_chunk_overlap = st.number_input(
                "Chunk Overlap:",
                min_value=0,
                max_value=1000,
                value=100,
                step=10,
                help="Overlap between adjacent chunks"
            )
            new_target_lag = st.selectbox(
                "Target Lag:",
                options=["1 minute", "2 minutes", "5 minutes", "10 minutes"],
                help="How quickly to process new files"
            )
        
        if st.button("➕ Create Document Type", type="primary"):
            if not new_file_type or not new_description:
                st.warning("Please fill in all required fields")
            else:
                # Validate file type format
                if not new_file_type.replace('_', '').replace('-', '').isalnum():
                    st.error("File type must contain only letters, numbers, hyphens, and underscores")
                else:
                    with st.spinner("Creating document type..."):
                        result = config_mgr.create_file_type_config(
                            new_file_type.upper(),
                            new_description,
                            new_chunk_size,
                            new_chunk_overlap,
                            new_target_lag
                        )
                        if result.get("success"):
                            st.success(f"✅ Document type '{new_file_type.upper()}' created successfully!")
                            st.info("💡 Don't forget to setup the processing pipeline for this document type.")
                            st.rerun()
                        else:
                            st.error(f"❌ Creation failed: {result.get('error', 'Unknown error')}")

def render_json_editor(config_json: Dict, key_prefix: str) -> Dict:
    """Render flexible JSON editor with both UI and text editing modes"""
    st.markdown("#### 📝 Configuration Editor")
    
    # Initialize session state for the full config
    config_key = f"{key_prefix}_config"
    json_text_key = f"{key_prefix}_json_text"
    edit_mode_key = f"{key_prefix}_edit_mode"
    
    if config_key not in st.session_state:
        st.session_state[config_key] = config_json.copy()
    if json_text_key not in st.session_state:
        st.session_state[json_text_key] = json.dumps(config_json, indent=2)
    if edit_mode_key not in st.session_state:
        st.session_state[edit_mode_key] = "UI Editor"
    
    # Editor mode selection
    edit_mode = st.radio(
        "Choose Editor Mode:",
        options=["UI Editor", "JSON Editor"],
        index=0 if st.session_state[edit_mode_key] == "UI Editor" else 1,
        key=f"{key_prefix}_mode_select",
        horizontal=True,
        help="UI Editor: Visual interface for easy editing | JSON Editor: Direct JSON text editing"
    )
    
    if edit_mode != st.session_state[edit_mode_key]:
        st.session_state[edit_mode_key] = edit_mode
        # Sync between modes when switching
        if edit_mode == "JSON Editor":
            st.session_state[json_text_key] = json.dumps(st.session_state[config_key], indent=2)
        else:
            try:
                st.session_state[config_key] = json.loads(st.session_state[json_text_key])
            except json.JSONDecodeError:
                pass  # Keep current config if JSON is invalid
        st.rerun()
    
    st.markdown("---")
    
    if edit_mode == "JSON Editor":
        # JSON Text Editor
        st.markdown("**📝 JSON Text Editor**")
        st.caption("Edit configuration as JSON - supports copy/paste and direct editing")
        
        json_text = st.text_area(
            "Configuration JSON:",
            value=st.session_state[json_text_key],
            height=400,
            key=f"{key_prefix}_json_text_area",
            help="Edit the JSON configuration directly. Make sure it's valid JSON."
        )
        
        # Update session state as user types
        if json_text != st.session_state[json_text_key]:
            st.session_state[json_text_key] = json_text
        
        # JSON validation and preview
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("🔍 Validate & Apply JSON", key=f"{key_prefix}_validate"):
                try:
                    parsed_json = json.loads(json_text)
                    st.success("✅ Valid JSON - Applied to configuration!")
                    st.session_state[config_key] = parsed_json
                    st.session_state[json_text_key] = json.dumps(parsed_json, indent=2)
                except json.JSONDecodeError as e:
                    st.error(f"❌ Invalid JSON: {str(e)}")
        
        with col2:
            if st.button("🔄 Switch to UI Editor", key=f"{key_prefix}_switch_to_ui"):
                try:
                    parsed_json = json.loads(json_text)
                    st.session_state[config_key] = parsed_json
                    st.session_state[json_text_key] = json.dumps(parsed_json, indent=2)
                    st.session_state[edit_mode_key] = "UI Editor"
                    st.rerun()
                except json.JSONDecodeError as e:
                    st.error(f"❌ Cannot switch: Invalid JSON - {str(e)}")
        
        # Show current parsed structure
        try:
            parsed_config = json.loads(json_text)
            st.markdown("**📋 Parsed Structure Preview:**")
            
            preview_data = []
            for key, value in parsed_config.items():
                if isinstance(value, dict):
                    preview_data.append({"Key": key, "Type": "Object", "Value": f"{len(value)} properties"})
                elif isinstance(value, list):
                    preview_data.append({"Key": key, "Type": "Array", "Value": f"{len(value)} items"})
                else:
                    preview_data.append({"Key": key, "Type": "String/Number", "Value": str(value)[:50] + "..." if len(str(value)) > 50 else str(value)})
            
            if preview_data:
                import pandas as pd
                df = pd.DataFrame(preview_data)
                st.dataframe(df, use_container_width=True, hide_index=True)
        except json.JSONDecodeError:
            st.info("💡 Enter valid JSON to see structure preview")
    
    else:
        # UI Editor - Frontend-focused editing
        st.markdown("**🎨 Visual Configuration Editor**")
        st.caption("Edit configuration using form controls - all changes happen instantly")
        
        # Render configuration fields
        current_config = st.session_state[config_key].copy()
        
        for main_key, main_value in current_config.items():
            # Use container instead of expander to avoid nesting issues
            with st.container():
                st.markdown(f"### 📁 {main_key.replace('_', ' ').title()}")
                
                # Main key editor
                col1, col2 = st.columns([4, 1])
                with col1:
                    new_main_key = st.text_input(
                        "Section Name:",
                        value=main_key,
                        key=f"{key_prefix}_main_{main_key}",
                        placeholder="section_name"
                    )
                    
                    # Update key if changed
                    if new_main_key != main_key and new_main_key not in current_config:
                        st.session_state[config_key][new_main_key] = st.session_state[config_key].pop(main_key)
                        st.rerun()
                
                with col2:
                    # Use a unique key for each delete button to avoid conflicts
                    delete_button_key = f"{key_prefix}_del_main_{main_key}_{hash(main_key)}"
                    if st.button("🗑️ Delete Section", key=delete_button_key, help="Delete entire section"):
                        # Directly delete from session state without queue
                        if main_key in st.session_state[config_key]:
                            del st.session_state[config_key][main_key]
                            # Update JSON representation immediately
                            st.session_state[json_text_key] = json.dumps(st.session_state[config_key], indent=2)
                            st.rerun()
                
                # Handle different value types
                if isinstance(main_value, dict):
                    # Object properties
                    st.markdown("**Properties:**")
                    
                    for prop_key, prop_value in main_value.items():
                        prop_col1, prop_col2, prop_col3 = st.columns([2, 5, 1])
                        
                        with prop_col1:
                            new_prop_key = st.text_input(
                                "Property:",
                                value=prop_key,
                                key=f"{key_prefix}_prop_{main_key}_{prop_key}",
                                placeholder="property_name"
                            )
                            
                            # Update property key if changed
                            if new_prop_key != prop_key and new_prop_key not in main_value:
                                if main_key in st.session_state[config_key]:
                                    st.session_state[config_key][main_key][new_prop_key] = st.session_state[config_key][main_key].pop(prop_key)
                                    st.rerun()
                        
                        with prop_col2:
                            new_prop_value = st.text_area(
                                "Value:",
                                value=str(prop_value),
                                key=f"{key_prefix}_val_{main_key}_{prop_key}",
                                height=80,
                                placeholder="Property value"
                            )
                            
                            # Update value immediately
                            if main_key in st.session_state[config_key]:
                                st.session_state[config_key][main_key][prop_key] = convert_string_to_appropriate_type(new_prop_value)
                        
                        with prop_col3:
                            st.markdown("<br>", unsafe_allow_html=True)
                            # Use unique key for property delete button
                            prop_delete_key = f"{key_prefix}_del_prop_{main_key}_{prop_key}_{hash(prop_key)}"
                            if st.button("🗑️", key=prop_delete_key, help="Delete property"):
                                # Directly delete property without queue
                                if main_key in st.session_state[config_key] and prop_key in st.session_state[config_key][main_key]:
                                    del st.session_state[config_key][main_key][prop_key]
                                    # Update JSON representation immediately
                                    st.session_state[json_text_key] = json.dumps(st.session_state[config_key], indent=2)
                                    st.rerun()
                    
                    # Add new property
                    st.markdown("---")
                    
                    # Use counter to force form recreation after successful submission
                    form_counter_key = f"{key_prefix}_form_counter_{main_key}"
                    if form_counter_key not in st.session_state:
                        st.session_state[form_counter_key] = 0
                    
                    # Use counter in form key to force recreation
                    form_key = f"{key_prefix}_add_prop_form_{main_key}_{st.session_state[form_counter_key]}"
                    
                    with st.form(key=form_key):
                        add_col1, add_col2, add_col3 = st.columns([2, 5, 1])
                        
                        with add_col1:
                            new_prop_key = st.text_input(
                                "New Property:",
                                value="",  # Always start with empty value
                                placeholder="new_property_name"
                            )
                        
                        with add_col2:
                            new_prop_value = st.text_input(
                                "Property Value:",
                                value="",  # Always start with empty value
                                placeholder="Property value"
                            )
                        
                        with add_col3:
                            st.markdown("<br>", unsafe_allow_html=True)
                            add_submitted = st.form_submit_button("➕ Add")
                            
                        if add_submitted and new_prop_key and new_prop_key not in st.session_state[config_key][main_key]:
                            st.session_state[config_key][main_key][new_prop_key] = convert_string_to_appropriate_type(new_prop_value)
                            # Update JSON representation
                            st.session_state[json_text_key] = json.dumps(st.session_state[config_key], indent=2)
                            # Increment counter to force form recreation with empty fields
                            st.session_state[form_counter_key] += 1
                            st.rerun()
                        elif add_submitted and new_prop_key in st.session_state[config_key][main_key]:
                            st.warning(f"Property '{new_prop_key}' already exists")
                        elif add_submitted and not new_prop_key:
                            st.warning("Please enter a property name")
                
                elif isinstance(main_value, list):
                    # Array items
                    st.markdown("**Array Items:**")
                    
                    for i, item in enumerate(main_value):
                        item_col1, item_col2 = st.columns([5, 1])
                        
                        with item_col1:
                            new_item_value = st.text_input(
                                f"Item {i+1}:",
                                value=str(item),
                                key=f"{key_prefix}_item_{main_key}_{i}",
                                placeholder="Array item value"
                            )
                            
                            # Update item immediately
                            if main_key in st.session_state[config_key] and i < len(st.session_state[config_key][main_key]):
                                st.session_state[config_key][main_key][i] = convert_string_to_appropriate_type(new_item_value)
                        
                        with item_col2:
                            # Use unique key for item delete button
                            item_delete_key = f"{key_prefix}_del_item_{main_key}_{i}_{hash(str(item))}"
                            if st.button("🗑️", key=item_delete_key, help="Delete item"):
                                # Directly delete item without queue
                                if main_key in st.session_state[config_key] and i < len(st.session_state[config_key][main_key]):
                                    st.session_state[config_key][main_key].pop(i)
                                    # Update JSON representation
                                    st.session_state[json_text_key] = json.dumps(st.session_state[config_key], indent=2)
                                    st.rerun()
                    
                    # Add new item
                    st.markdown("---")
                    
                    # Use counter to force form recreation after successful submission
                    item_form_counter_key = f"{key_prefix}_item_form_counter_{main_key}"
                    if item_form_counter_key not in st.session_state:
                        st.session_state[item_form_counter_key] = 0
                    
                    # Use counter in form key to force recreation
                    item_form_key = f"{key_prefix}_add_item_form_{main_key}_{st.session_state[item_form_counter_key]}"
                    
                    with st.form(key=item_form_key):
                        add_item_col1, add_item_col2 = st.columns([5, 1])
                        
                        with add_item_col1:
                            new_item = st.text_input(
                                "New Item:",
                                value="",  # Always start with empty value
                                placeholder="New array item"
                            )
                        
                        with add_item_col2:
                            add_item_submitted = st.form_submit_button("➕ Add")
                            
                        if add_item_submitted and new_item:
                            st.session_state[config_key][main_key].append(convert_string_to_appropriate_type(new_item))
                            # Update JSON representation
                            st.session_state[json_text_key] = json.dumps(st.session_state[config_key], indent=2)
                            # Increment counter to force form recreation with empty fields
                            st.session_state[item_form_counter_key] += 1
                            st.rerun()
                        elif add_item_submitted and not new_item:
                            st.warning("Please enter an item value")
                
                else:
                    # Simple value
                    new_simple_value = st.text_area(
                        "Value:",
                        value=str(main_value),
                        key=f"{key_prefix}_simple_{main_key}",
                        height=100,
                        placeholder="Configuration value"
                    )
                    
                    # Update value immediately
                    st.session_state[config_key][main_key] = convert_string_to_appropriate_type(new_simple_value)
        
        # Add new top-level section
        st.markdown("---")
        st.markdown("**➕ Add New Configuration Section**")
        
        # Use counter to force form recreation after successful submission
        section_form_counter_key = f"{key_prefix}_section_form_counter"
        if section_form_counter_key not in st.session_state:
            st.session_state[section_form_counter_key] = 0
        
        # Use counter in form key to force recreation
        section_form_key = f"{key_prefix}_add_section_form_{st.session_state[section_form_counter_key]}"
        
        with st.form(key=section_form_key):
            add_section_col1, add_section_col2, add_section_col3 = st.columns([2, 2, 2])
            
            with add_section_col1:
                new_section_key = st.text_input(
                    "Section Name:",
                    value="",  # Always start with empty value
                    placeholder="new_section_name"
                )
            
            with add_section_col2:
                new_section_type = st.selectbox(
                    "Section Type:",
                    ["Simple Value", "Object (Properties)", "Array (List)"],
                    index=0
                )
            
            with add_section_col3:
                st.markdown("<br>", unsafe_allow_html=True)
                add_section_submitted = st.form_submit_button("➕ Add Section")
                
            if add_section_submitted and new_section_key and new_section_key not in st.session_state[config_key]:
                if new_section_type == "Object (Properties)":
                    st.session_state[config_key][new_section_key] = {"new_property": "New value"}
                elif new_section_type == "Array (List)":
                    st.session_state[config_key][new_section_key] = ["New item"]
                else:
                    st.session_state[config_key][new_section_key] = "New value"
                # Update JSON representation
                st.session_state[json_text_key] = json.dumps(st.session_state[config_key], indent=2)
                # Increment counter to force form recreation with empty fields
                st.session_state[section_form_counter_key] += 1
                st.rerun()
            elif add_section_submitted and new_section_key in st.session_state[config_key]:
                st.warning(f"Section '{new_section_key}' already exists")
            elif add_section_submitted and not new_section_key:
                st.warning("Please enter a section name")
        
        # Update JSON text representation continuously
        st.session_state[json_text_key] = json.dumps(st.session_state[config_key], indent=2)
        
        # Switch to JSON editor button
        st.markdown("---")
        if st.button("📝 Switch to JSON Editor", key=f"{key_prefix}_switch_to_json"):
            st.session_state[edit_mode_key] = "JSON Editor"
            st.rerun()
    
    # Return the current configuration
    return st.session_state[config_key].copy()

def render_processing_config_management(config_mgr: ConfigurationManager):
    """Render the processing configuration management tab"""
    st.subheader("🔧 Processing Configuration Management")
    st.markdown("Create and manage AI processing configurations")
    
    # Load existing configurations
    processing_configs = config_mgr.get_processing_configs()
    
    # Display existing configurations
    if processing_configs:
        st.markdown("### Existing Processing Configurations")
        
        for config in processing_configs:
            # Use container instead of expander to avoid nesting issues with JSON editor
            with st.container():
                st.markdown(f"#### ⚙️ {config['CONFIG_NAME']} ({config['PROCESSING_TYPE']})")
                
                # Show/hide toggle for each config
                show_config = st.toggle(
                    f"Show Details",
                    key=f"show_config_{config['CONFIG_NAME']}",
                    help=f"Toggle to show/hide details for {config['CONFIG_NAME']}"
                )
                
                if show_config:
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.write(f"**Name:** {config['CONFIG_NAME']}")
                        st.write(f"**Type:** {config['PROCESSING_TYPE']}")
                        st.write(f"**Created:** {config['CREATED_AT']}")
                    
                    with col2:
                        st.write(f"**Model:** {config['CONFIG_MODEL']}")
                        st.write(f"**Parameters:** {len(config['CONFIG_JSON'].get('extraction_config', {}))}")
                        st.write(f"**Search Limit:** {config['CONFIG_JSON'].get('search_limit', 'N/A')}")
                        st.write(f"**Updated:** {config['UPDATED_AT']}")
                    
                    # Action buttons
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        if st.button(f"✏️ Edit", key=f"edit_proc_{config['CONFIG_NAME']}"):
                            st.session_state[f"editing_proc_{config['CONFIG_NAME']}"] = True
                            st.rerun()
                    
                    with col2:
                        if st.button(f"🧪 Test", key=f"test_proc_{config['CONFIG_NAME']}"):
                            validation = config_mgr.validate_processing_config(config['CONFIG_JSON'])
                            if validation.get("is_valid"):
                                st.success("✅ Configuration is valid!")
                                if validation.get("warnings"):
                                    for warning in validation["warnings"]:
                                        st.warning(f"⚠️ {warning}")
                            else:
                                st.error("❌ Configuration validation failed:")
                                for error in validation.get("errors", []):
                                    st.error(f"❌ {error}")
                    
                    with col3:
                        if st.button(f"🗑️ Delete", key=f"delete_proc_{config['CONFIG_NAME']}", type="secondary"):
                            st.session_state[f"confirm_delete_proc_{config['CONFIG_NAME']}"] = True
                            st.rerun()
                    
                    # Edit form
                    if st.session_state.get(f"editing_proc_{config['CONFIG_NAME']}", False):
                        st.markdown("---")
                        st.markdown("**Edit Configuration:**")
                        
                        # Processing type selection
                        edit_col1, edit_col2 = st.columns(2)
                        
                        with edit_col1:
                            new_processing_type = st.selectbox(
                                "Processing Type:",
                                options=["CORTEX_SEARCH", "AI_EXTRACT"],
                                index=["CORTEX_SEARCH", "AI_EXTRACT"].index(config['PROCESSING_TYPE']),
                                key=f"edit_type_{config['CONFIG_NAME']}"
                            )
                        
                        with edit_col2:
                            # Model selection - fetch available models dynamically
                            models_data = config_mgr.get_available_models()
                            model_options = models_data.get("supported_models", ["claude-4-sonnet"])
                            
                            current_model_index = 0
                            if config['CONFIG_MODEL'] in model_options:
                                current_model_index = model_options.index(config['CONFIG_MODEL'])
                            
                            new_config_model = st.selectbox(
                                "AI Model:",
                                options=model_options,
                                index=current_model_index,
                                key=f"edit_model_{config['CONFIG_NAME']}",
                                help=f"Recommended: Quality={models_data.get('recommended_models', {}).get('quality', 'claude-4-sonnet')}, Speed={models_data.get('recommended_models', {}).get('speed', 'llama3-8b')}"
                            )
                        
                        # JSON editor
                        updated_config = render_json_editor(
                            config['CONFIG_JSON'],
                            f"edit_{config['CONFIG_NAME']}"
                        )
                        
                        # Save/Cancel buttons
                        save_col1, save_col2 = st.columns(2)
                        
                        with save_col1:
                            if st.button(f"💾 Save Changes", key=f"save_proc_{config['CONFIG_NAME']}"):
                                # Validate before saving
                                validation = config_mgr.validate_processing_config(updated_config)
                                if validation.get("is_valid"):
                                    result = config_mgr.update_processing_config(
                                        config['CONFIG_NAME'],
                                        new_processing_type,
                                        new_config_model,
                                        updated_config
                                    )
                                    if result.get("success"):
                                        st.success("✅ Configuration updated successfully!")
                                        st.session_state[f"editing_proc_{config['CONFIG_NAME']}"] = False
                                        st.rerun()
                                    else:
                                        st.error(f"❌ Update failed: {result.get('error', 'Unknown error')}")
                                else:
                                    st.error("❌ Configuration validation failed:")
                                    for error in validation.get("errors", []):
                                        st.error(f"❌ {error}")
                        
                        with save_col2:
                            if st.button(f"❌ Cancel", key=f"cancel_proc_{config['CONFIG_NAME']}"):
                                st.session_state[f"editing_proc_{config['CONFIG_NAME']}"] = False
                                st.rerun()
                    
                    # Delete confirmation
                    if st.session_state.get(f"confirm_delete_proc_{config['CONFIG_NAME']}", False):
                        st.markdown("---")
                        st.warning(f"⚠️ **Confirm Deletion of '{config['CONFIG_NAME']}'**")
                        
                        del_col1, del_col2 = st.columns(2)
                        
                        with del_col1:
                            if st.button(f"🗑️ Confirm Delete", key=f"confirm_proc_{config['CONFIG_NAME']}", type="primary"):
                                result = config_mgr.delete_processing_config(config['CONFIG_NAME'])
                                if result.get("success"):
                                    st.success(f"✅ Configuration '{config['CONFIG_NAME']}' deleted successfully!")
                                    st.session_state[f"confirm_delete_proc_{config['CONFIG_NAME']}"] = False
                                    st.rerun()
                                else:
                                    st.error(f"❌ Deletion failed: {result.get('error', 'Unknown error')}")
                        
                        with del_col2:
                            if st.button(f"❌ Cancel Delete", key=f"cancel_delete_proc_{config['CONFIG_NAME']}"):
                                st.session_state[f"confirm_delete_proc_{config['CONFIG_NAME']}"] = False
                                st.rerun()
                
                st.markdown("---")  # Add separator between configs
    
    st.markdown("---")
    
    # Add new processing configuration
    st.markdown("### ➕ Create New Processing Configuration")
    
    # Use container and toggle instead of expander to avoid nesting issues with JSON editor
    show_create_form = st.toggle(
        "Show Create New Configuration Form",
        key="show_create_config_form",
        help="Toggle to show/hide the new configuration creation form"
    )
    
    if show_create_form:
        new_config_name = st.text_input(
            "Configuration Name:",
            placeholder="e.g., Standard_Contract_Analysis",
            help="Unique name for this configuration"
        )
        
        col1, col2 = st.columns(2)
        
        with col1:
            new_processing_type = st.selectbox(
                "Processing Type:",
                options=["CORTEX_SEARCH", "AI_EXTRACT"],
                help="CORTEX_SEARCH: Uses semantic search + AI_COMPLETE | AI_EXTRACT: Uses AI_EXTRACT for bulk processing"
            )
        
        with col2:
            # Model selection - fetch available models dynamically
            models_data = config_mgr.get_available_models()
            model_options = models_data.get("supported_models", ["claude-4-sonnet"])
            quality_model = models_data.get("recommended_models", {}).get("quality", "claude-4-sonnet")
            
            # Set default index to quality recommendation
            default_index = 0
            if quality_model in model_options:
                default_index = model_options.index(quality_model)
            
            new_config_model = st.selectbox(
                "AI Model:",
                options=model_options,
                index=default_index,
                help=f"Choose the AI model for document processing. Recommended: Quality={quality_model}, Speed={models_data.get('recommended_models', {}).get('speed', 'llama3-8b')}"
            )
        
        # Default configuration template
        default_config = {
            "extraction_config": {
                "effective_date": "What is the effective date or start date of this agreement?",
                "agreement_duration": "What is the duration or term of this agreement?",
                "payment_terms": "What are the payment terms?"
            },
            "evaluation_config": {
                "effective_date": "Does the SOW start date fall within the MSA effective period?",
                "agreement_duration": "Is the SOW duration within the MSA term limits?",
                "payment_terms": "Do the SOW payment terms comply with MSA requirements?"
            },
            "search_limit": 3
        }
        
        # JSON editor for new configuration
        new_config_json = render_json_editor(default_config, "new_config")
        
        if st.button("➕ Create Configuration", type="primary"):
            if not new_config_name:
                st.warning("Please enter a configuration name")
            else:
                # Validate configuration
                validation = config_mgr.validate_processing_config(new_config_json)
                if validation.get("is_valid"):
                    result = config_mgr.create_processing_config(
                        new_config_name,
                        new_processing_type,
                        new_config_model,
                        new_config_json
                    )
                    if result.get("success"):
                        st.success(f"✅ Configuration '{new_config_name}' created successfully!")
                        st.rerun()
                    else:
                        st.error(f"❌ Creation failed: {result.get('error', 'Unknown error')}")
                else:
                    st.error("❌ Configuration validation failed:")
                    for error in validation.get("errors", []):
                        st.error(f"❌ {error}")

def render_pipeline_management(config_mgr: ConfigurationManager):
    """Render the pipeline management tab"""
    st.subheader("🚧 Pipeline Management")
    st.markdown("Monitor and manage document processing pipelines")
    
    # Get file type configurations for pipeline management
    file_configs = config_mgr.get_file_type_configs()
    
    if not file_configs:
        st.info("No document types configured. Please add document types first.")
        return
    
    # Global pipeline status
    st.markdown("### 📊 Pipeline Status Overview")
    
    status_data = []
    for config in file_configs:
        file_type = config['FILE_TYPE']
        status = config_mgr.check_pipeline_status(file_type)
        
        if "error" not in status:
            objects = status.get("objects", {})
            data_info = status.get("data", {})
            
            status_data.append({
                "File Type": file_type,
                "Stage": "✅" if objects.get("stage", {}).get("exists") else "❌",
                "Stream": "✅" if objects.get("stream", {}).get("exists") else "❌",
                "Task": "✅" if objects.get("task", {}).get("exists") else "❌",
                "Search Service": "✅" if objects.get("search_service", {}).get("exists") else "❌",
                "Files": data_info.get("total_files", 0),
                "Chunks": data_info.get("total_chunks", 0)
            })
        else:
            status_data.append({
                "File Type": file_type,
                "Stage": "❓",
                "Stream": "❓", 
                "Task": "❓",
                "Search Service": "❓",
                "Files": 0,
                "Chunks": 0
            })
    
    if status_data:
        df = pd.DataFrame(status_data)
        st.dataframe(df, use_container_width=True, hide_index=True)
    
    st.markdown("---")
    
    # Individual pipeline management
    st.markdown("### 🔧 Individual Pipeline Management")
    
    selected_file_type = st.selectbox(
        "Select Document Type:",
        options=[config['FILE_TYPE'] for config in file_configs],
        help="Choose a document type to manage its pipeline"
    )
    
    if selected_file_type:
        # Get configuration for selected file type
        selected_config = next((c for c in file_configs if c['FILE_TYPE'] == selected_file_type), None)
        
        if selected_config:
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.markdown(f"**📄 {selected_file_type}**")
                st.write(f"Description: {selected_config['FILE_DESCRIPTION']}")
                st.write(f"Chunk Size: {selected_config['CHUNK_SIZE']}")
                st.write(f"Chunk Overlap: {selected_config['CHUNK_OVERLAP']}")
                st.write(f"Target Lag: {selected_config['TARGET_LAG']}")
            
            with col2:
                if st.button("🚀 Setup Pipeline", key=f"setup_pipeline_{selected_file_type}"):
                    with st.spinner(f"Setting up pipeline for {selected_file_type}..."):
                        result = config_mgr.setup_file_processing_pipeline(
                            selected_file_type,
                            selected_config['CHUNK_SIZE'],
                            selected_config['CHUNK_OVERLAP'],
                            selected_config['TARGET_LAG']
                        )
                        if result.get("success"):
                            st.success(f"✅ Pipeline setup completed for {selected_file_type}")
                            with st.expander("Setup Details"):
                                st.write("**Objects Created:**")
                                st.write(f"• Stage: {result.get('stage_name')}")
                                st.write(f"• Stream: {result.get('stream_name')}")
                                st.write(f"• Task: {result.get('task_name')}")
                                st.write(f"• Search Service: {result.get('cortex_search_service_name')}")
                                st.write(f"• Chunks Table: {result.get('chunks_table_name')}")
                        else:
                            st.error(f"❌ Pipeline setup failed: {result.get('error', 'Unknown error')}")
            
            with col3:
                if st.button("📊 Check Status", key=f"check_status_{selected_file_type}"):
                    status = config_mgr.check_pipeline_status(selected_file_type)
                    if "error" not in status:
                        st.success("✅ Pipeline status retrieved")
                        
                        st.write("**Pipeline Objects:**")
                        objects = status.get("objects", {})
                        for obj_name, obj_info in objects.items():
                            status_icon = "✅" if obj_info.get("exists") else "❌"
                            st.write(f"{status_icon} {obj_name.replace('_', ' ').title()}")
                            
                            # Show additional details for specific objects
                            if obj_name == "task" and obj_info.get("exists"):
                                st.write(f"   State: {obj_info.get('state', 'Unknown')}")
                                st.write(f"   Last Run: {obj_info.get('last_run', 'Never')}")
                            elif obj_name == "stream" and obj_info.get("exists"):
                                has_data = "Yes" if obj_info.get("has_data") else "No"
                                st.write(f"   Has Data: {has_data}")
                        
                        # Show data statistics
                        if "data" in status:
                            data_info = status["data"]
                            st.write("**Data Statistics:**")
                            st.write(f"• Total Files: {data_info.get('total_files', 0)}")
                            st.write(f"• Total Chunks: {data_info.get('total_chunks', 0)}")
                            st.write(f"• First Processed: {data_info.get('first_processed', 'Never')}")
                            st.write(f"• Last Processed: {data_info.get('last_processed', 'Never')}")
                    else:
                        st.error(f"❌ Status check failed: {status['error']}")
            
            st.markdown("---")
            
            # Sync files options
            with st.expander("🔄 Sync Files from Stage", expanded=False):
                st.info("💡 **File Sync Utility**")
                st.write("Manually process files from stage that may have been missed by the stream. Useful when:")
                st.write("• Stream processing failed or was interrupted")
                st.write("• Files were uploaded directly to stage outside of normal workflow")
                st.write("• You want to reprocess files with different parameters")
                
                sync_col1, sync_col2 = st.columns(2)
                
                with sync_col1:
                    sync_chunk_size = st.number_input(
                        "Chunk Size:",
                        min_value=100,
                        max_value=5000,
                        value=selected_config['CHUNK_SIZE'],
                        step=100,
                        key=f"sync_chunk_size_{selected_file_type}",
                        help="Size of text chunks for processing"
                    )
                    
                    sync_chunk_overlap = st.number_input(
                        "Chunk Overlap:",
                        min_value=0,
                        max_value=1000,
                        value=selected_config['CHUNK_OVERLAP'],
                        step=10,
                        key=f"sync_chunk_overlap_{selected_file_type}",
                        help="Overlap between adjacent chunks"
                    )
                
                with sync_col2:
                    force_reprocess = st.checkbox(
                        "Force Reprocess",
                        value=False,
                        key=f"force_reprocess_{selected_file_type}",
                        help="Reprocess all files by truncating the chunks table and recreating the stream. This completely reloads all document data."
                    )
                    
                    if force_reprocess:
                        st.warning("⚠️ **Force Reprocess**: This will TRUNCATE the chunks table and recreate the stream, then reprocess ALL files in the stage")
                    else:
                        st.info("ℹ️ **Smart Sync**: Only process files not yet in chunks table")
                
                if st.button(f"🔄 Sync Files for {selected_file_type}", type="primary", key=f"sync_files_{selected_file_type}"):
                    with st.spinner(f"Syncing files for {selected_file_type}..."):
                        sync_result = config_mgr.sync_files(
                            selected_file_type,
                            sync_chunk_size,
                            sync_chunk_overlap,
                            force_reprocess
                        )
                        
                        if sync_result.get("success"):
                            st.success("✅ File sync completed successfully!")
                            
                            # Display sync summary
                            col1, col2, col3, col4 = st.columns(4)
                            
                            with col1:
                                st.metric("Files Found", sync_result.get("files_found", 0))
                            
                            with col2:
                                st.metric("Files Processed", sync_result.get("files_processed", 0))
                            
                            with col3:
                                st.metric("Files Skipped", sync_result.get("files_skipped", 0))
                            
                            with col4:
                                st.metric("Chunks Created", sync_result.get("chunks_created", 0))
                            
                            # Show detailed results
                            st.write(f"**Message:** {sync_result.get('message', 'Sync completed')}")
                            
                            if sync_result.get("search_service_refreshed"):
                                st.success("🔍 Search service refreshed automatically")
                            
                            # Show processing details if available
                            processing_details = sync_result.get("processing_details", [])
                            if processing_details:
                                # Use toggle instead of nested expander to avoid UI error
                                show_details = st.toggle("📋 Show Processing Details", key=f"show_details_{selected_file_type}")
                                
                                if show_details:
                                    st.markdown("**Processing Details:**")
                                    for detail in processing_details:
                                        status_icon = {
                                            "processed": "✅",
                                            "skipped": "⏭️", 
                                            "failed": "❌",
                                            "error": "🚨"
                                        }.get(detail.get("status", "unknown"), "❓")
                                        
                                        st.write(f"{status_icon} **{detail.get('file_name', 'Unknown')}**")
                                        
                                        if detail.get("status") == "processed":
                                            st.write(f"   Chunks created: {detail.get('chunks_created', 0)}")
                                            st.write(f"   File size: {detail.get('file_size', 'Unknown')} bytes")
                                        elif detail.get("status") == "skipped":
                                            st.write(f"   Reason: {detail.get('reason', 'Unknown')}")
                                        elif detail.get("status") in ["failed", "error"]:
                                            st.write(f"   Error: {detail.get('error', detail.get('reason', 'Unknown'))}")
                                        
                                        st.write("")  # Add spacing
                        else:
                            st.error(f"❌ File sync failed: {sync_result.get('error', 'Unknown error')}")
            
            st.markdown("---")
            
            # Cleanup options
            with st.expander("🗑️ Cleanup Pipeline", expanded=False):
                st.warning("⚠️ **Pipeline Cleanup**")
                st.write("This will remove all pipeline objects (stage, stream, task, search service) for this document type.")
                
                drop_data_checkbox = st.checkbox(
                    "Also delete all processed data (chunks table and content)",
                    key=f"drop_data_checkbox_{selected_file_type}",
                    help="WARNING: This will permanently delete all processed documents and chunks"
                )
                
                if drop_data_checkbox:
                    st.error("⚠️ **DATA DELETION WARNING**: This will permanently delete all processed content!")
                
                if st.button(f"🗑️ Cleanup Pipeline for {selected_file_type}", type="secondary", key=f"cleanup_{selected_file_type}"):
                    with st.spinner(f"Cleaning up pipeline for {selected_file_type}..."):
                        result = config_mgr.delete_file_type_config(selected_file_type, drop_data_checkbox)
                        if result.get("success"):
                            st.success(f"✅ Pipeline cleanup completed for {selected_file_type}")
                            st.write("**Actions performed:**")
                            for action in result.get("actions", []):
                                st.write(f"• {action}")
                        else:
                            st.error(f"❌ Cleanup failed: {result.get('error', 'Unknown error')}")

def main():
    """Main configuration page"""
    st.title("⚙️ Files Configuration Management")
    st.markdown("Configure document types, processing configurations, and manage pipelines")
    st.markdown("---")
    
    # Initialize configuration manager
    config_mgr = ConfigurationManager()
    
    # Create tabs
    tab1, tab2, tab3 = st.tabs(["📁 Document Types", "🔧 Processing Configs", "🚧 Pipeline Management"])
    
    with tab1:
        render_file_type_management(config_mgr)
    
    with tab2:
        render_processing_config_management(config_mgr)
    
    with tab3:
        render_pipeline_management(config_mgr)

if __name__ == "__main__":
    main() 