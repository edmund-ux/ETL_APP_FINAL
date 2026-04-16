import streamlit as st
import time
import boto3
import pandas as pd
from db_utils import setup_logger, create_database, get_schema_info

# Page Configuration
st.set_page_config(
    page_title="DB Management Portal",
    page_icon="🗄️",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Custom CSS for that extra professional, minimalistic dark feel
st.markdown("""
    <style>
    /* Sleek typography */
    h1, h2, h3 { font-family: 'Inter', sans-serif; color: #FFFFFF; }
    /* Primary accent color for title */
    .title-highlight { color: #00FFAE; }
    /* Inputs padding */
    .stTextInput > div > div > input { background-color: rgba(30, 33, 39, 0.6); color: white; border: 1px solid #333; border-radius: 10px; }
    .stNumberInput > div > div > input { background-color: rgba(30, 33, 39, 0.6); color: white; border: 1px solid #333; border-radius: 10px; }
    /* Glassmorphism for containers */
    div[data-testid="stVerticalBlockBorderWrapper"] {
        background: rgba(30, 33, 39, 0.45);
        border-radius: 16px !important;
        box-shadow: 0 4px 30px rgba(0, 0, 0, 0.3);
        backdrop-filter: blur(10px);
        -webkit-backdrop-filter: blur(10px);
        border: 1px solid rgba(255, 255, 255, 0.05) !important;
    }
    /* Beautiful submit button with rounded corners */
    .stButton>button {
        width: 100%;
        border-radius: 30px !important;
        background-color: #00FFAE;
        color: #0E1117;
        font-weight: bold;
        transition: 0.3s;
        border: none;
        box-shadow: 0 4px 15px rgba(0, 255, 174, 0.2);
    }
    .stButton>button:hover { 
        background-color: #00E59A; 
        color: #0E1117;
        box-shadow: 0 6px 20px rgba(0, 255, 174, 0.4);
        transform: translateY(-2px);
    }
    /* Log console styling */
    .stCode { background-color: #06080A !important; border-left: 4px solid #00FFAE; border-radius: 8px; }
    </style>
""", unsafe_allow_html=True)

st.markdown("<h1><span class='title-highlight'>🗄️ Database Management</span> Portal</h1>", unsafe_allow_html=True)
st.markdown("Automated, secure provisioning of PostgreSQL databases on AWS RDS using IAM Authentication.")
st.divider()

# Sidebar Status Badge
with st.sidebar:
    st.subheader("☁️ System Identity")
    try:
        sts = boto3.client('sts')
        identity = sts.get_caller_identity()
        user_arn = identity.get('Arn', 'Unknown User')
        
        st.markdown(
            f"""
            <div style="background-color: rgba(0, 255, 174, 0.1); border: 1px solid #00FFAE; padding: 12px; border-radius: 12px; display: flex; align-items: center; gap: 12px; margin-bottom: 10px;">
                <div style="width: 12px; height: 12px; background-color: #00FFAE; border-radius: 50%; box-shadow: 0 0 10px #00FFAE; flex-shrink: 0;"></div>
                <div style="font-weight: 600; color: #00FFAE; font-size: 0.95rem;">AWS Connected</div>
            </div>
            <div style="font-size: 0.8rem; color: #888; background: #16181C; padding: 10px; border-radius: 8px; word-break: break-all;">
                <strong>Identity:</strong><br/>{user_arn}
            </div>
            """,
            unsafe_allow_html=True
        )
    except Exception as e:
        st.markdown(
            """
            <div style="background-color: rgba(255, 51, 102, 0.1); border: 1px solid #FF3366; padding: 12px; border-radius: 12px; display: flex; align-items: center; gap: 12px; margin-bottom: 10px;">
                <div style="width: 12px; height: 12px; background-color: #FF3366; border-radius: 50%; box-shadow: 0 0 10px #FF3366; flex-shrink: 0;"></div>
                <div style="font-weight: 600; color: #FF3366; font-size: 0.95rem;">Disconnected</div>
            </div>
            <div style="font-size: 0.8rem; color: #888; background: #16181C; padding: 10px; border-radius: 8px;">
                <strong>Error:</strong><br/>No valid AWS credentials found. Please authenticate via AWS CLI.
            </div>
            """,
            unsafe_allow_html=True
        )

# Layout Tabs
tab_deploy, tab_schema = st.tabs(["🚀 Deployment Path", "🔍 Schema Browser"])

with tab_deploy:
    col1, col2 = st.columns([1, 1.2], gap="large")
    
    with col1:
        st.subheader("🛠️ Deployment Configuration")
        
        with st.container(border=True):
            with st.form("deploy_form"):
                db_name = st.text_input("New Database Name", placeholder="e.g., my_analytics_db", help="Name must be lowercase, no spaces.")
                
                st.markdown("#### Database Identity & Location")
                
                # Using endpoint provided by the user in the prompt
                host = st.text_input("AWS RDS Endpoint", value="database-1-instance-1.czc24c00qzf0.us-east-1.rds.amazonaws.com")
                
                col_port, col_user = st.columns(2)
                with col_port:
                    port = st.number_input("Port", value=5432, step=1)
                with col_user:
                    user = st.text_input("RDS Username", value="postgres", help="An IAM-enabled database user")
                    
                region = st.text_input("AWS Region", value="us-east-1", help="The region where your RDS instance is hosted")
                
                submit = st.form_submit_button("🚀 Execute Database Creation")

    with col2:
        st.subheader("📋 View Logs")
        st.markdown("Real-time telemetry and handshake status for your AWS RDS deployment.")
        
        # We create a placeholder where our custom logging handler will write
        log_placeholder = st.empty()
        logger = setup_logger(log_placeholder)
        
        if not submit:
             # Initial default state
             log_placeholder.code("System Idle. Awaiting deployment...", language="text")

    # Execution logic outside columns so balloons run globally over the UI
    if submit:
        if not db_name.strip():
            st.error("Deployment Aborted: Please enter a valid Database Name.")
            logger.error("Deployment triggered with empty Database Name.")
        else:
            # Show progress spinner while everything happens
            with st.spinner(f"Connecting to {host}..."):
                try:
                    # The heavy lifting
                    success = create_database(host, int(port), user, region, db_name, logger)
                    
                    if success:
                        st.success(f"Database Initialization Complete! The database '{db_name}' is ready for use.")
                        st.toast('Deployment Successful!', icon='✅')
                        st.balloons()
                except Exception as e:
                    st.error(f"Deployment encountered a critical error. Check logs for details.")
                    st.toast('Deployment Failed', icon='❌')

with tab_schema:
    st.subheader("🔍 Schema Explorer")
    st.markdown("Inspect tables and columns residing securely on your PostgreSQL RDS instance.")
    
    with st.container(border=True):
        with st.form("schema_form"):
            col_db, col_host = st.columns([1, 1.5])
            with col_db:
                schema_db_name = st.text_input("Target Database", placeholder="e.g., my_analytics_db")
            with col_host:
                schema_host = st.text_input("RDS Endpoint", value="database-1-instance-1.czc24c00qzf0.us-east-1.rds.amazonaws.com")
            
            col_port2, col_user2, col_region2 = st.columns(3)
            with col_port2:
                schema_port = st.number_input("Port", value=5432, step=1, key="schema_port")
            with col_user2:
                schema_user = st.text_input("IAM Username", value="postgres", key="schema_user")
            with col_region2:
                schema_region = st.text_input("Region", value="us-east-1", key="schema_region")
            
            schema_submit = st.form_submit_button("Fetch Schema")
            
    if schema_submit:
        if not schema_db_name.strip():
            st.warning("Please enter a Target Database name to inspect.")
        else:
            with st.spinner(f"Connecting to {schema_db_name} and fetching metadata..."):
                try:
                    schema_data = get_schema_info(schema_host, int(schema_port), schema_user, schema_region, schema_db_name)
                    
                    if not schema_data:
                        st.info(f"Database '{schema_db_name}' exists, but has no tables.")
                    else:
                        st.success(f"Successfully retrieved metadata for {len(schema_data)} table(s).")
                        
                        # Render tables as expanders containing dataframes
                        for table_name, columns in schema_data.items():
                            with st.expander(f"🗃️ Table: {table_name}", expanded=True):
                                df = pd.DataFrame(columns)
                                st.dataframe(df, use_container_width=True, hide_index=True)
                except Exception as e:
                    st.error(f"Failed to fetch schema. Make sure the database exists and you have correct permissions.")
                    st.exception(e)

