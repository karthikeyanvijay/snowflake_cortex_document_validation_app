#!/bin/bash

# Deploy Streamlit App to Snowflake
cd ../streamlit
snow streamlit deploy --replace --database FROSTLOGIC_DB --schema PUBLIC 