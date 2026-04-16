import boto3
import sqlalchemy
from sqlalchemy import create_engine
import time
import logging
from urllib.parse import quote_plus

class StreamlitLogHandler(logging.Handler):
    """Custom logging handler to send logs directly to a Streamlit placeholder"""
    def __init__(self, log_placeholder):
        super().__init__()
        self.log_placeholder = log_placeholder
        self.log_text = ""

    def emit(self, record):
        log_entry = self.format(record)
        self.log_text += f"{log_entry}\n"
        self.log_placeholder.code(self.log_text, language='text')

def setup_logger(log_placeholder):
    logger = logging.getLogger("rds_deployer")
    logger.setLevel(logging.INFO)
    
    # Remove old handlers to prevent duplicate logs on rerun
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
        
    sh = StreamlitLogHandler(log_placeholder)
    formatter = logging.Formatter('%(asctime)s - [%(levelname)s] - %(message)s', datefmt='%H:%M:%S')
    sh.setFormatter(formatter)
    logger.addHandler(sh)
    return logger

def generate_iam_token(host: str, port: int, user: str, region: str, logger: logging.Logger):
    logger.info(f"Initiating IAM token generation for RDS instance...")
    try:
        session = boto3.Session(region_name=region)
        client = session.client('rds')
        
        logger.info(f"Requesting DB auth token for user '{user}' on port {port}")
        token = client.generate_db_auth_token(
            DBHostname=host,
            Port=port,
            DBUsername=user,
            Region=region
        )
        # Obfuscate token slightly in logs for security
        obfuscated_token = token[:10] + "..." + token[-5:] if token else "None"
        logger.info(f"Successfully generated IAM DB Auth Token: {obfuscated_token}")
        return token
    except Exception as e:
        logger.error(f"Failed to generate IAM token: {str(e)}")
        raise e

def create_database(host: str, port: int, user: str, region: str, db_name: str, logger: logging.Logger):
    logger.info(f"Starting database creation sequence for '{db_name}'...")
    
    # Step 1: Get the IAM token
    token = generate_iam_token(host, port, user, region, logger)
    
    # Step 2: Formulate connection string
    logger.info(f"Building SQLAlchemy connection string utilizing IAM token...")
    encoded_token = quote_plus(token)
    
    # Connecting to default 'postgres' db to create the new database
    db_url = f"postgresql+psycopg2://{user}:{encoded_token}@{host}:{port}/postgres"
    
    # Connect with SSL args required for IAM auth typically
    # We pass 'sslmode=require' in connect_args
    engine = create_engine(
        db_url,
        connect_args={'sslmode': 'require'}
    )
    
    logger.info("Attempting RDS handshake...")
    time.sleep(0.5) # Slight delay for visual logging progress
    
    try:
        # Step 3: Connect with AUTOCOMMIT (necessary for CREATE DATABASE)
        with engine.execution_options(isolation_level="AUTOCOMMIT").connect() as conn:
            logger.info("Handshake successful. Connected to AWS RDS cluster.")
            
            # Additional check if DB exists
            logger.info(f"Checking if database '{db_name}' already exists...")
            check_db = conn.execute(
                sqlalchemy.text("SELECT 1 FROM pg_database WHERE datname = :name"),
                {"name": db_name}
            ).fetchone()
            
            if check_db:
                logger.warning(f"Database '{db_name}' already exists on the instance.")
                raise Exception(f"Database '{db_name}' already exists.")
            
            # Issue CREATE DATABASE command
            # Note: Parameterized query doesn't work for CREATE DATABASE identifiers in SQLAlchemy
            logger.info(f"Executing: CREATE DATABASE \"{db_name}\";")
            time.sleep(1) # Simulation of deployment waiting for user
            conn.execute(sqlalchemy.text(f"CREATE DATABASE \"{db_name}\""))
            
            logger.info(f"Success! Database '{db_name}' has been created on {host}.")
            return True
            
    except Exception as e:
        logger.error(f"Database creation failed: {str(e)}")
        raise e

def get_schema_info(host: str, port: int, user: str, region: str, db_name: str, logger: logging.Logger = None):
    """Connects to a specific database and returns its table schema using SQLAlchemy Inspector."""
    if logger:
        logger.info(f"Fetching schema for database '{db_name}'...")
        
    token = generate_iam_token(host, port, user, region, logger if logger else logging.getLogger())
    encoded_token = quote_plus(token)
    # Important: Connect directly to the target db_name, not 'postgres'
    db_url = f"postgresql+psycopg2://{user}:{encoded_token}@{host}:{port}/{db_name}"
    
    engine = create_engine(db_url, connect_args={'sslmode': 'require'})
    
    try:
        inspector = sqlalchemy.inspect(engine)
        tables = inspector.get_table_names()
        
        schema_data = {}
        for table in tables:
            columns = inspector.get_columns(table)
            schema_data[table] = [{"name": col['name'], "type": str(col['type'])} for col in columns]
            
        if logger:
            logger.info(f"Successfully retrieved schema for '{db_name}'. Found {len(tables)} tables.")
        return schema_data
    except Exception as e:
        if logger:
            logger.error(f"Failed to fetch schema: {str(e)}")
        raise e
