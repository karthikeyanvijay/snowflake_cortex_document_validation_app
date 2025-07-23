#!/bin/bash

# Frostlogic Complete Setup Script
# Executes all setup steps (01-08) in sequence
# Usage: ./run_all_setup.sh [--connection <connection_name>]

set -e  # Exit on any error

# Parse command line arguments
CONNECTION_PARAM=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --connection)
            CONNECTION_PARAM="--connection $2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--connection <connection_name>]"
            exit 1
            ;;
    esac
done

echo "🚀 Starting Frostlogic Complete Setup..."
echo "========================================"

if [ -n "$CONNECTION_PARAM" ]; then
    echo "Using connection: $CONNECTION_PARAM"
else
    echo "Using default connection"
fi

echo ""

# Step 01: Create Tables
echo "📋 Step 01: Creating database tables and infrastructure..."
snow sql -f 01_create_tables.sql $CONNECTION_PARAM
echo "✅ Step 01 completed"
echo ""

# Step 02: Parse Documents (Setup procedures)
echo "📄 Step 02: Setting up document parsing procedures..."
snow sql -f 02_parse_documents.sql $CONNECTION_PARAM
echo "✅ Step 02 completed"
echo ""

# Step 03: Create Comparison Procedures
echo "🔍 Step 03: Creating comparison procedures..."
snow sql -f 03_create_comparison_procedure.sql $CONNECTION_PARAM
echo "✅ Step 03 completed"
echo ""

# Step 04: Setup Pipelines
echo "⚙️ Step 04: Setting up processing pipelines..."
snow sql -f 04_setup_pipelines.sql $CONNECTION_PARAM
echo "✅ Step 04 completed"
echo ""

# Step 05: Upload Test Files (if exists)
if [ -f "05_upload_test_files.sh" ]; then
    echo "📁 Step 05: Uploading test files..."
    if [ -n "$CONNECTION_PARAM" ]; then
        # Modify the script to use the connection parameter
        sed "s/snow stage copy/snow stage copy $CONNECTION_PARAM/g" 05_upload_test_files.sh > temp_upload.sh
        chmod +x temp_upload.sh
        ./temp_upload.sh
        rm temp_upload.sh
    else
        chmod +x 05_upload_test_files.sh
        ./05_upload_test_files.sh
    fi
    echo "✅ Step 05 completed"
    echo ""
else
    echo "⏭️ Step 05: Test files upload script not found, skipping..."
    echo ""
fi

# Step 06: API Tests (if exists)
if [ -f "06_api_tests.sql" ]; then
    echo "🧪 Step 06: Running API tests..."
    snow sql -f 06_api_tests.sql $CONNECTION_PARAM
    echo "✅ Step 06 completed"
    echo ""
else
    echo "⏭️ Step 06: API tests not found, skipping..."
    echo ""
fi

# Step 07: Management Procedures
echo "🛠️ Step 07: Setting up management procedures..."
snow sql -f 07_management_procedures.sql $CONNECTION_PARAM
echo "✅ Step 07 completed"
echo ""

# Step 08: Deploy Streamlit App
echo "🎨 Step 08: Deploying Streamlit application..."
cd ../streamlit
if [ -n "$CONNECTION_PARAM" ]; then
    snow streamlit deploy --replace --database FROSTLOGIC_DB --schema PUBLIC $CONNECTION_PARAM
else
    snow streamlit deploy --replace --database FROSTLOGIC_DB --schema PUBLIC
fi
cd ../setup
echo "✅ Step 08 completed"
echo ""

echo "🎉 Frostlogic Complete Setup Finished Successfully!"
echo "=================================================="
echo ""
echo "📋 Summary:"
echo "   ✅ Database tables and infrastructure created"
echo "   ✅ Document parsing procedures deployed"
echo "   ✅ Comparison procedures deployed"
echo "   ✅ Processing pipelines configured"
echo "   ✅ Test files uploaded (if available)"
echo "   ✅ API tests executed (if available)"
echo "   ✅ Management procedures deployed"
echo "   ✅ Streamlit application deployed"
echo ""
echo "🔗 Next Steps:"
echo "   1. Access your Streamlit app in Snowflake"
echo "   2. Configure document types in the Configuration page"
echo "   3. Upload documents using File Management page"
echo "   4. Start processing documents!"
echo ""
echo "📖 For more information, see README.md" 