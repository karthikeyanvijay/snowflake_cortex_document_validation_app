# Document Validation App - Setup Guide

This directory contains all setup scripts to deploy the ** Document Validation App** - a Snowflake-native application that uses Cortex AI to analyze and validate documents such as (MSAs and SOWs.

## 🏗️ System Architecture

**Frostlogic** creates:
- **Database**: `FROSTLOGIC_DB` with document processing tables
- **AI Processing**: Cortex Search Services and AI_COMPLETE functions
- **Web Interface**: Streamlit application for document analysis
- **Document Storage**: Snowflake stages for PDF/DOCX files
- **Automated Workflows**: Tasks for real-time document processing

---

## 📋 Prerequisites

### Required Snowflake Features
- ✅ **Snowflake Account** with Cortex AI enabled
- ✅ **ACCOUNTADMIN** access (for initial setup only)
- ✅ **Enterprise Edition** or higher

### Development Tools (Choose One Path)
- **Option A**: [Snow CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli/index) (recommended for automation)
- **Option B**: [Snowsight Web Interface](https://docs.snowflake.com/en/user-guide/ui-snowsight) (GUI approach)

---

## 🔐 Step 1: Admin Setup (REQUIRED)

**⚠️ IMPORTANT**: Before any deployment, an **ACCOUNTADMIN** must run the admin setup script.

### Using Snow CLI:
```bash
# Connect as ACCOUNTADMIN
snow connection add --connection-name admin_connection
# Enter ACCOUNTADMIN credentials when prompted

# Run admin setup
snow sql -f 00_admin_setup.sql --connection admin_connection
```

### Using Snowsight:
1. **Login to Snowsight** as **ACCOUNTADMIN**
2. **Navigate to**: Worksheets → New Worksheet
3. **Copy and paste** contents from `00_admin_setup.sql`
4. **Execute** the script

### What the Admin Setup Creates:
- **Role**: `FROSTLOGIC_ROLE` with necessary permissions
- **Database**: `FROSTLOGIC_DB` with ownership grants
- **Privileges**: Account-level permissions for tasks and Cortex AI

---

## 🛤️ Path A: Snow CLI Deployment

**Best for**: DevOps, CI/CD, automated deployments, and power users.

### Prerequisites
```bash
# Install Snow CLI
pip install snowflake-cli-labs

# Verify installation
snow --version
```

### Setup Connection
```bash
# Create connection for Frostlogic deployment
snow connection add --connection-name frostlogic

# Enter your credentials:
# - Account: your-account.region.cloud-provider
# - Username: your-username  
# - Password: your-password
# - Role: FROSTLOGIC_ROLE
# - Warehouse: FROSTLOGIC_WH
# - Database: FROSTLOGIC_DB
# - Schema: PUBLIC

# Test connection
snow connection test --connection frostlogic
```

### Automated Deployment
```bash
cd setup

# Option 1: Run everything automatically
./run_all_setup.sh --connection frostlogic

# Option 2: Run with custom connection
./run_all_setup.sh --connection your_connection_name

# Option 3: Step-by-step deployment
snow sql -f 01_create_tables.sql --connection frostlogic
snow sql -f 02_parse_documents.sql --connection frostlogic  
snow sql -f 03_create_comparison_procedure.sql --connection frostlogic
snow sql -f 04_setup_pipelines.sql --connection frostlogic
./05_upload_test_files.sh --connection frostlogic
snow sql -f 06_api_tests.sql --connection frostlogic
snow sql -f 07_management_procedures.sql --connection frostlogic
./08_deploy_streamlit.sh --connection frostlogic
```

### Deploy Streamlit App
```bash
cd ../streamlit

# Deploy Streamlit application
snow streamlit deploy --replace \
  --database FROSTLOGIC_DB \
  --schema PUBLIC \
  --connection frostlogic

# Get application URL
snow streamlit get-url FROSTLOGIC_STREAMLIT_APP \
  --database FROSTLOGIC_DB \
  --schema PUBLIC \
  --connection frostlogic
```

---

## 🖥️ Path B: Snowsight Deployment

**Best for**: GUI users, one-time setups, and learning the system.

### Step 1: Database & Tables Setup
1. **Login to Snowsight** with your Frostlogic-enabled account
2. **Switch Role**: Use role `FROSTLOGIC_ROLE`
3. **Navigate to**: Worksheets → New Worksheet
4. **Set Context**:
   ```sql
   USE ROLE FROSTLOGIC_ROLE;
   USE DATABASE FROSTLOGIC_DB;
   USE SCHEMA PUBLIC;
   USE WAREHOUSE FROSTLOGIC_WH;
   ```

### Step 2: Execute Setup Scripts (In Order)
Execute each script in a **new worksheet** or clear previous results:

#### 1. Database Infrastructure
- **Copy** contents of `01_create_tables.sql`
- **Paste** into worksheet and **Run All**
- **Verify**: Check that tables `FILE_TYPE_CONFIG` exists

#### 2. Document Processing
- **Copy** contents of `02_parse_documents.sql`  
- **Paste** into worksheet and **Run All**
- **Verify**: Procedures like `PROCESS_FILES_FROM_STREAM` are created

#### 3. AI Comparison Procedures
- **Copy** contents of `03_create_comparison_procedure.sql`
- **Paste** into worksheet and **Run All**
- **Verify**: Procedures like `COMPARE_FILES` and `GET_AVAILABLE_MODELS` exist

#### 4. Processing Pipelines
- **Copy** contents of `04_setup_pipelines.sql`
- **Paste** into worksheet and **Run All**
- **Verify**: Tasks and streams are created

#### 5. Upload Test Files (Manual)
- **Navigate to**: Data → Databases → FROSTLOGIC_DB → PUBLIC → Stages
- **Find stages**: `MSA_STAGE` and `SOW_STAGE`
- **Upload sample PDFs**:
  - Upload `sample_docs/msa/*.pdf` to `MSA_STAGE`
  - Upload `sample_docs/sow/*.pdf` to `SOW_STAGE`
- **Wait 3-5 minutes** for automatic processing

#### 6. API Validation Tests
- **Copy** contents of `06_api_tests.sql`
- **Paste** into worksheet and **Run All** 
- **Verify**: All tests pass successfully

#### 7. Management Procedures
- **Copy** contents of `07_management_procedures.sql`
- **Paste** into worksheet and **Run All**
- **Verify**: Configuration management procedures exist

### Step 3: Deploy Streamlit Application

#### 1. Create Streamlit Stage
1. **Open**: Worksheets → New Worksheet
2. **Run**: 
   ```sql
   -- Create stage for Streamlit files
   CREATE STAGE IF NOT EXISTS STREAMLIT_STAGE
   DIRECTORY = (ENABLE = TRUE)
   ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
   COMMENT = 'Stage for Frostlogic Streamlit application files';
   ```

#### 2. Upload Streamlit Files via Snowsight
1. **Navigate to**: Data → Databases → FROSTLOGIC_DB → PUBLIC → Stages
2. **Find**: `STREAMLIT_STAGE` and click on it
3. **Upload Files** (use the upload interface in Snowsight):
   - Upload `streamlit/streamlit_app.py` to root of stage
   - Upload `streamlit/environment.yml` to root of stage
   - Create `pages` folder and upload:
     - Upload `streamlit/pages/01_Configuration.py` to `pages/` folder
     - Upload `streamlit/pages/02_Upload_Files.py` to `pages/` folder

#### 3. Create Streamlit Object
1. **Open**: Worksheets → New Worksheet
2. **Run**: 
   ```sql
   -- Create Streamlit application object
   CREATE STREAMLIT FROSTLOGIC_STREAMLIT_APP
   ROOT_LOCATION = '@STREAMLIT_STAGE'
   MAIN_FILE = 'streamlit_app.py'
   QUERY_WAREHOUSE = FROSTLOGIC_WH;
   ```
3. **Verify**: Run `SHOW STREAMLITS;` to confirm creation

---

## ✅ Verification & Testing

### Application Access
1. **Find your app**: Projects → Streamlit Apps → FROSTLOGIC_STREAMLIT_APP  
2. **Or direct URL**: `https://app.snowflake.com/[region]/[account]/#/streamlit-apps/FROSTLOGIC_DB.PUBLIC.FROSTLOGIC_STREAMLIT_APP`


---

## 📁 Script Reference

| Script | Purpose | Required Role |
|--------|---------|---------------|
| `00_admin_setup.sql` | Create roles and database | ACCOUNTADMIN |
| `01_create_tables.sql` | Database tables and stages | FROSTLOGIC_ROLE |
| `02_parse_documents.sql` | Document processing logic | FROSTLOGIC_ROLE |
| `03_create_comparison_procedure.sql` | AI comparison procedures | FROSTLOGIC_ROLE |
| `04_setup_pipelines.sql` | Automated processing tasks | FROSTLOGIC_ROLE |
| `05_upload_test_files.sh` | Sample document upload | FROSTLOGIC_ROLE |
| `06_api_tests.sql` | System validation tests | FROSTLOGIC_ROLE |
| `07_management_procedures.sql` | Configuration management | FROSTLOGIC_ROLE |
| `08_deploy_streamlit.sh` | Streamlit app deployment | FROSTLOGIC_ROLE |
| `run_all_setup.sh` | Complete automated setup | FROSTLOGIC_ROLE |
