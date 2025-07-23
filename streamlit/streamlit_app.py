import streamlit as st
import json
import pandas as pd
from datetime import datetime
from snowflake.snowpark.context import get_active_session
from typing import Dict, List, Any, Optional

st.set_page_config(
    page_title='Frostlogic Document Validation System',
    page_icon='❄️',
    layout='wide',
    initial_sidebar_state='expanded'
)

class FrostlogicApp:
    def __init__(self):
        self.session = get_active_session()
        
    def _render_document_selector(self, index: int, available_types: List[str]) -> Optional[Dict[str, str]]:
        """Render a single document selector interface"""
        # Document type selection
        doc_type_key = f"doc_type_{index}"
        doc_type = st.selectbox(
            "Document Type:",
            options=["None"] + available_types,
            key=doc_type_key,
            help=f"Select document type for document {index + 1}"
        )
        
        if doc_type != "None":
            # File selection for this document type
            files = self.get_files_by_type(doc_type)
            
            if files:
                file_options = {f"{f['FILE_NAME']} ({f['CHUNK_COUNT']} chunks)": f for f in files}
                
                file_key = f"file_{index}"
                selected_file_key = st.selectbox(
                    "Select File:",
                    options=list(file_options.keys()),
                    key=file_key,
                    help=f"Choose file from {doc_type} stage"
                )
                
                selected_file = file_options[selected_file_key]
                
                # Display file info in a more compact format
                st.success(f"📄 **{selected_file['FILE_NAME']}**")
                st.caption(f"Chunks: {selected_file['CHUNK_COUNT']} | Updated: {selected_file['LAST_PROCESSED']}")
                
                # Return document info
                return {
                    "file_name": selected_file['STAGE_PATH'],  # Use stage path for backend (e.g., @MSA_STAGE/file.pdf)
                    "file_type": doc_type
                }
            else:
                st.warning(f"No files found for {doc_type}")
                return None
        else:
            st.info("No document selected")
            return None

    def get_processing_configs(self) -> List[Dict[str, Any]]:
        """Fetch all processing configurations"""
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
    
    def get_available_models(self) -> Dict[str, Any]:
        """Get all available AI models"""
        try:
            result = self.session.sql("CALL GET_AVAILABLE_MODELS()").collect()
            if result:
                models_data = result[0]['GET_AVAILABLE_MODELS']
                if isinstance(models_data, str):
                    models_data = json.loads(models_data)
                return models_data
            return {"supported_models": [], "default_model": "claude-4-sonnet", "recommended_models": {}}
        except Exception as e:
            st.error(f"Failed to load available models: {str(e)}")
            # Return fallback models if procedure fails
            return {
                "supported_models": [
                    "claude-4-sonnet",
                    "claude-3-5-sonnet", 
                    "claude-3-7-sonnet",
                    "llama3-8b",
                    "mixtral-8x7b",
                    "snowflake-llama-3.1-405b"
                ],
                "default_model": "claude-4-sonnet",
                "recommended_models": {
                    "quality": "claude-4-sonnet",
                    "balanced": "mixtral-8x7b",
                    "speed": "llama3-8b"
                }
            }
    
    def get_available_file_types(self) -> List[str]:
        """Get all available file types"""
        try:
            result = self.session.sql("CALL AVAILABLE_FILE_TYPES_GET()").collect()
            return [row["FILE_TYPE"] for row in result]
        except Exception as e:
            st.error(f"Failed to load file types: {str(e)}")
            return []
    
    def get_files_by_type(self, file_type: str) -> List[Dict[str, Any]]:
        """Get files for a specific document type with proper stage paths"""
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
            st.error(f"Failed to load files for {file_type}: {str(e)}")
            return []
    
    def process_documents(self, file_config: List[Dict], processing_config: Dict, model_name: str = 'claude-4-sonnet') -> Dict[str, Any]:
        """Process documents using the appropriate procedure"""
        try:
            # Determine which procedure to use based on processing type
            processing_type = processing_config['PROCESSING_TYPE']
            config_json = processing_config['CONFIG_JSON']
            
            # Convert file_config to JSON string for procedure call
            file_config_json = json.dumps(file_config)
            config_json_str = json.dumps(config_json)
            
            # Escape quotes for SQL
            file_config_safe = file_config_json.replace("'", "''")
            config_json_safe = config_json_str.replace("'", "''")
            
            if processing_type == 'CORTEX_SEARCH':
                procedure_name = 'COMPARE_FILES'
            elif processing_type == 'AI_EXTRACT':
                procedure_name = 'COMPARE_FILES_AISQL'
            else:
                return {"error": f"Unknown processing type: {processing_type}"}
            
            # Call the appropriate procedure
            sql = f"""
            CALL {procedure_name}(
                PARSE_JSON('{file_config_safe}'),
                PARSE_JSON('{config_json_safe}'),
                '{model_name}'
            )
            """
            
            result = self.session.sql(sql).collect()
            
            if result:
                response_data = result[0][procedure_name]
                if isinstance(response_data, str):
                    response_data = json.loads(response_data)
                return response_data
            
            return {"error": "No response from procedure"}
            
        except Exception as e:
            return {"error": f"Processing failed: {str(e)}"}

    def display_results(self, results: Dict[str, Any]):
        """Display processing results with visualizations for multi-file analysis"""
        if "error" in results:
            st.error(f"❌ Processing Error: {results['error']}")
            return
        
        if not results.get("success", False):
            st.error("❌ Processing failed")
            return
        
        st.success("✅ Processing completed successfully!")
        
        # Create summary metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Files Analyzed", len(results.get("files_analyzed", [])))
        
        with col2:
            st.metric("Analysis Type", results.get("analysis_type", "Unknown").title())
        
        with col3:
            avg_score = results.get("summary", {}).get("average_evaluation_score", 0.0)
            st.metric("Avg Evaluation Score", f"{avg_score:.2f}")
        
        with col4:
            high_matches = results.get("summary", {}).get("high_evaluation_matches", 0)
            st.metric("High Score Matches", high_matches)
        
        st.markdown("---")
        
        # Check if we have multiple files for enhanced visualizations
        files_analyzed = results.get("files_analyzed", [])
        num_files = len(files_analyzed)
        
        # Display multi-file visualizations if we have more than one file
        if num_files > 1 and "results" in results and results["results"]:
            self._display_multi_file_visualizations(results)
            st.markdown("---")
        
        # Display detailed results
        if "results" in results and results["results"]:
            st.subheader("📊 Detailed Analysis Results")
            
            # Convert results to DataFrame for better display
            table_data = []
            for category, data in results["results"].items():
                row = {
                    "Category": category.replace('_', ' ').title(),
                    "Extraction Question": data.get("extraction_question", "N/A")[:100] + "..." if len(data.get("extraction_question", "")) > 100 else data.get("extraction_question", "N/A")
                }
                
                # Add file answers
                file_answers = data.get("file_answers", {})
                for file_name, answer in file_answers.items():
                    # Clean file name for display (handle stage paths)
                    clean_name = file_name.split('/')[-1] if '/' in file_name else file_name
                    if clean_name.startswith('@'):
                        clean_name = clean_name.split('/')[-1] if '/' in clean_name else clean_name
                    row[f"Answer ({clean_name})"] = answer[:150] + "..." if len(str(answer)) > 150 else str(answer)
                
                # Add evaluation results
                evaluation = data.get("evaluation", {})
                row["Evaluation Score"] = f"{evaluation.get('evaluation_score', 0.0):.2f}"
                row["Evaluation Explanation"] = evaluation.get('evaluation_explanation', 'N/A')[:100] + "..." if len(evaluation.get('evaluation_explanation', '')) > 100 else evaluation.get('evaluation_explanation', 'N/A')
                
                table_data.append(row)
            
            if table_data:
                df = pd.DataFrame(table_data)
                st.dataframe(df, use_container_width=True, hide_index=True)
                
                # Download options
                st.markdown("### 📥 Download Results")
                col1, col2 = st.columns(2)
                
                with col1:
                    csv = df.to_csv(index=False)
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    st.download_button(
                        label="📄 Download as CSV",
                        data=csv,
                        file_name=f"frostlogic_results_{timestamp}.csv",
                        mime="text/csv"
                    )
                
                with col2:
                    json_str = json.dumps(results, indent=2)
                    st.download_button(
                        label="📋 Download Raw JSON",
                        data=json_str,
                        file_name=f"frostlogic_results_{timestamp}.json",
                        mime="application/json"
                    )
        
        # Show processing metadata
        with st.expander("🔍 Processing Details"):
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("**Model Used:**", results.get("model_used", "Unknown"))
                st.write("**Timestamp:**", results.get("timestamp", "Unknown"))
                st.write("**Extraction Method:**", results.get("extraction_method", "Standard"))
            
            with col2:
                files_analyzed = results.get("files_analyzed", [])
                st.write("**Files Processed:**")
                for file_info in files_analyzed:
                    # Extract filename from stage path for display
                    file_path = file_info.get('file_name', 'Unknown')
                    file_display = file_path.split('/')[-1] if '/' in file_path else file_path
                    if file_display.startswith('@'):
                        file_display = file_display.split('/')[-1] if '/' in file_display else file_display
                    st.write(f"- {file_display} ({file_info.get('file_type', 'Unknown')})")

    def _display_multi_file_visualizations(self, results: Dict[str, Any]):
        """Display visualizations for multi-file analysis"""
        try:
            import plotly.express as px
            import plotly.graph_objects as go
            from plotly.subplots import make_subplots
        except ImportError:
            st.warning("📊 Plotly not available - visualizations disabled. Install plotly to enable charts.")
            return
        
        st.subheader("📈 Multi-File Analysis Visualizations")
        
        comparison_results = results.get("results", {})
        files_analyzed = results.get("files_analyzed", [])
        
        # Create clean file name mapping
        clean_file_names = {}
        for file_info in files_analyzed:
            file_path = file_info.get('file_name', '')
            clean_name = file_path.split('/')[-1] if '/' in file_path else file_path
            if clean_name.startswith('@'):
                clean_name = clean_name.split('/')[-1] if '/' in clean_name else clean_name
            clean_file_names[file_path] = clean_name
        
        # Check if we have evaluation scores
        has_evaluations = any("evaluation" in data and data["evaluation"].get("evaluation_score") is not None 
                             for data in comparison_results.values())
        
        # 1. Evaluation Scores Visualization
        if has_evaluations:
            self._create_evaluation_charts(comparison_results, clean_file_names)

    def _create_evaluation_charts(self, comparison_results: Dict, clean_file_names: Dict):
        """Create evaluation score visualizations"""
        try:
            import plotly.express as px
            import plotly.graph_objects as go
        except ImportError:
            return
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### 📊 Evaluation Scores by Category")
            
            # Prepare data for evaluation scores chart
            eval_data = []
            for category, data in comparison_results.items():
                evaluation = data.get("evaluation", {})
                if "evaluation_score" in evaluation:
                    eval_data.append({
                        'Category': category.replace('_', ' ').title(),
                        'Score': evaluation['evaluation_score']
                    })
            
            if eval_data:
                df_eval = pd.DataFrame(eval_data)
                
                # Create horizontal bar chart
                fig = px.bar(
                    df_eval, 
                    x='Score', 
                    y='Category',
                    orientation='h',
                    color='Score',
                    color_continuous_scale=['red', 'yellow', 'green'],
                    range_color=[0, 1],
                    title="Compliance Scores by Category"
                )
                
                fig.update_layout(
                    height=400,
                    yaxis={'categoryorder': 'total ascending'},
                    xaxis_title="Compliance Score (0-1)"
                )
                
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("#### 🎯 Score Distribution")
            
            if eval_data:
                scores = [item['Score'] for item in eval_data]
                
                # Create score distribution
                fig = go.Figure()
                
                fig.add_trace(go.Histogram(
                    x=scores,
                    nbinsx=min(10, len(scores)),
                    name="Score Distribution",
                    marker_color='lightblue',
                    opacity=0.7
                ))
                
                fig.update_layout(
                    title="Distribution of Compliance Scores",
                    xaxis_title="Compliance Score",
                    yaxis_title="Number of Categories",
                    height=400
                )
                
                # Add threshold lines
                fig.add_vline(x=0.8, line_dash="dash", line_color="green", 
                             annotation_text="Excellent (0.8+)")
                fig.add_vline(x=0.6, line_dash="dash", line_color="orange", 
                             annotation_text="Good (0.6+)")
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Show average score with status
                avg_score = sum(scores) / len(scores)
                if avg_score >= 0.8:
                    st.success(f"🌟 **Average Score: {avg_score:.2f}** - Excellent overall compliance!")
                elif avg_score >= 0.6:
                    st.warning(f"⚡ **Average Score: {avg_score:.2f}** - Good overall compliance")
                else:
                    st.error(f"⚠️ **Average Score: {avg_score:.2f}** - Compliance needs improvement")





    def run(self):
        """Main application interface"""
        st.title("❄️ Document Validation ❄️")
        st.markdown("Process and validate documents using AI-powered analysis")
        st.markdown("---")
        
        # Initialize session state
        if 'processing_results' not in st.session_state:
            st.session_state.processing_results = None
        if 'selected_files' not in st.session_state:
            st.session_state.selected_files = {}
        
        # Sidebar configuration
        with st.sidebar:
            st.header("⚙️ Processing Configuration")
            
            # Load processing configurations
            processing_configs = self.get_processing_configs()
            
            if not processing_configs:
                st.error("No processing configurations available. Please configure them first.")
                st.stop()
            
            # Configuration selection
            config_options = {f"{config['CONFIG_NAME']} ({config['PROCESSING_TYPE']})": config for config in processing_configs}
            selected_config_key = st.selectbox(
                "Select Processing Configuration:",
                options=list(config_options.keys()),
                help="Choose the AI processing method and parameter configuration"
            )
            
            selected_config = config_options[selected_config_key]
            
            # Model selection with override capability
            st.markdown("**AI Model Selection:**")
            
            # Get available models
            models_data = self.get_available_models()
            available_models = models_data.get("supported_models", ["claude-4-sonnet"])
            config_model = selected_config['CONFIG_MODEL']
            
            # Find index of config model in available models, default to 0 if not found
            try:
                default_index = available_models.index(config_model)
            except ValueError:
                default_index = 0
                if config_model not in available_models:
                    available_models.insert(0, config_model)  # Add config model if not in list
                    default_index = 0
            
            selected_model = st.selectbox(
                "Model:",
                options=available_models,
                index=default_index,
                key="model_selection",
                help="Choose the AI model for processing. Default is from configuration, but you can override it here."
            )
            
            # Store selected model in session state for access outside sidebar
            st.session_state.selected_model = selected_model
            
            # Show if model is overridden
            if selected_model != config_model:
                st.info(f"📝 Model overridden: {config_model} → **{selected_model}**")
            else:
                st.success(f"✅ Using config model: **{selected_model}**")
            
            # Model recommendations box
            recommended_models = models_data.get("recommended_models", {})
            if recommended_models:
                with st.expander("🎯 Recommended Models", expanded=False):
                    st.markdown("**Choose by use case:**")
                    
                    # Primary recommendations in a structured layout
                    if any(key in recommended_models for key in ["quality", "balanced", "speed"]):
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            if "quality" in recommended_models:
                                st.markdown("🌟 **Quality**")
                                st.code(recommended_models['quality'])
                                st.caption("Best accuracy & reasoning")
                                st.markdown("")
                            
                            if "speed" in recommended_models:
                                st.markdown("⚡ **Speed**")
                                st.code(recommended_models['speed'])
                                st.caption("Fastest processing")
                        
                        with col2:
                            if "balanced" in recommended_models:
                                st.markdown("⚖️ **Balanced**")
                                st.code(recommended_models['balanced'])
                                st.caption("Good speed & quality")
                                st.markdown("")
                            
                            # Additional recommendations in second column
                            if "reasoning" in recommended_models:
                                st.markdown("🧠 **Advanced Reasoning**")
                                st.code(recommended_models['reasoning'])
                                st.caption("Complex analysis tasks")
                    
                    # Enterprise recommendation spans full width if available
                    if "enterprise" in recommended_models:
                        st.markdown("---")
                        st.markdown("🏢 **Enterprise Grade**")
                        st.code(recommended_models['enterprise'])
                        st.caption("Production workloads & compliance")
            
            # Show configuration details
            with st.expander("📋 Configuration Details"):
                st.write(f"**Type:** {selected_config['PROCESSING_TYPE']}")
                st.write(f"**Config Model:** {selected_config['CONFIG_MODEL']}")
                st.write(f"**Selected Model:** {selected_model}")
                st.write(f"**Parameters:** {len(selected_config['CONFIG_JSON'].get('extraction_config', {}))}")
                st.write(f"**Search Limit:** {selected_config['CONFIG_JSON'].get('search_limit', 'N/A')}")
            
            st.markdown("---")
        
        # Main content area
        st.subheader("📁 Document Selection")
        
        # Dynamic number selection
        col_config, col_info = st.columns([1, 2])
        
        with col_config:
            num_documents = st.number_input(
                "Number of documents to compare:",
                min_value=1,
                max_value=2,
                value=st.session_state.get('num_documents', 2),
                step=1,
                help="Choose how many documents you want to analyze (maximum 2)"
            )
            
            # Clear document selections if number changed
            if st.session_state.get('num_documents') != num_documents:
                # Clear all document selection keys when count changes
                keys_to_clear = [key for key in st.session_state.keys() if key.startswith(('doc_type_', 'file_'))]
                for key in keys_to_clear:
                    del st.session_state[key]
                st.session_state.processing_results = None  # Also clear results
            
            st.session_state.num_documents = num_documents
        
        with col_info:
            if num_documents == 1:
                st.info("📄 **Single Document Analysis** - Extract information from one document")
            elif num_documents == 2:
                st.info("📊 **Comparative Analysis** - Compare and evaluate two documents for compliance")
            else:
                st.info(f"📚 **Multi-Document Analysis** - Analyze {num_documents} documents together for patterns and compliance")
            
            # Show tip for optimal selection
            if num_documents <= 3:
                st.caption("💡 Tip: Up to 2 documents display side-by-side for easy comparison")
            else:
                st.caption("💡 Tip: More than 2 documents use compact layout for better readability")
        
        st.markdown("---")
        
        # Get available file types
        available_types = self.get_available_file_types()
        
        if not available_types:
            st.error("No document types configured. Please set up document types in the Configuration page.")
            st.stop()
        
        # Dynamic document selection interface
        selected_documents = []
        
        # Create dynamic columns based on user selection
        if num_documents <= 3:
            # For 1-3 documents, use columns for horizontal layout
            columns = st.columns(num_documents)
            
            for i in range(num_documents):
                with columns[i]:
                    st.markdown(f"**Document {i+1}**")
                    doc_info = self._render_document_selector(i, available_types)
                    if doc_info:
                        selected_documents.append(doc_info)
        else:
            # For 4-5 documents, use a more compact vertical layout
            st.markdown("**Document Selection:**")
            
            # Create rows of 2 columns each for better space utilization
            for row in range((num_documents + 1) // 2):
                cols = st.columns(2)
                for col_idx in range(2):
                    doc_idx = row * 2 + col_idx
                    if doc_idx < num_documents:
                        with cols[col_idx]:
                            st.markdown(f"**Document {doc_idx + 1}**")
                            doc_info = self._render_document_selector(doc_idx, available_types)
                            if doc_info:
                                selected_documents.append(doc_info)
        
        st.markdown("---")
        
        # Processing controls
        if selected_documents:
            # Display selected documents in a more compact format
            st.success(f"✅ **{len(selected_documents)} of {num_documents} documents selected**")
            
            # Show selected files in columns for better layout
            if len(selected_documents) <= 3:
                display_cols = st.columns(len(selected_documents))
                for i, (doc, col) in enumerate(zip(selected_documents, display_cols)):
                    with col:
                        # Extract just the filename from the stage path (@STAGE/path/file.pdf -> file.pdf)
                        file_display = doc['file_name'].split('/')[-1] if '/' in doc['file_name'] else doc['file_name']
                        if file_display.startswith('@'):
                            file_display = file_display.split('/')[-1] if '/' in file_display else file_display
                        st.write(f"**{i+1}.** {file_display}")
                        st.caption(f"Type: {doc['file_type']}")
            else:
                # For more than 3 documents, use a more compact list
                for i, doc in enumerate(selected_documents):
                    # Extract just the filename from the stage path (@STAGE/path/file.pdf -> file.pdf)
                    file_display = doc['file_name'].split('/')[-1] if '/' in doc['file_name'] else doc['file_name']
                    if file_display.startswith('@'):
                        file_display = file_display.split('/')[-1] if '/' in file_display else file_display
                    st.write(f"**{i+1}.** {file_display} ({doc['file_type']})")
        else:
            if num_documents > 0:
                st.warning(f"⚠️ Please select {num_documents} document{'s' if num_documents > 1 else ''} to continue")
            else:
                st.info("Configure document selection above")
        
        # Action buttons
        col1, col2 = st.columns([1, 1])
        
        with col1:
            process_disabled = len(selected_documents) == 0
            button_text = "🚀 Process Documents" if len(selected_documents) > 1 else "🚀 Analyze Document"
            
            if st.button(button_text, disabled=process_disabled, use_container_width=True, type="primary"):
                if selected_documents:
                    # Get the selected model from session state
                    model_to_use = st.session_state.get('selected_model', selected_config['CONFIG_MODEL'])
                    
                    with st.spinner("Processing documents... This may take a few minutes."):
                        results = self.process_documents(
                            file_config=selected_documents,
                            processing_config=selected_config,
                            model_name=model_to_use
                        )
                        st.session_state.processing_results = results
                        st.rerun()
        
        with col2:
            if st.button("🔄 Clear Results", use_container_width=True):
                st.session_state.processing_results = None
                st.rerun()
        
        # Display results if available
        if st.session_state.processing_results:
            st.markdown("---")
            st.subheader("📊 Processing Results")
            self.display_results(st.session_state.processing_results)

if __name__ == "__main__":
    app = FrostlogicApp()
    app.run() 