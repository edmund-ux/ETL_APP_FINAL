import asyncio
from mcp.server.fastmcp import FastMCP
from db_utils import get_schema_info, generate_iam_token
import sqlalchemy
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import logging

# Initialize a standard Python logger for the MCP
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mcp_server")

# Create the FastMCP Server
mcp = FastMCP("AWS RDS PostgreSQL MCP Server")

@mcp.tool()
def list_tables(host: str, port: int, user: str, region: str, db_name: str) -> list:
    """List all tables in the specified AWS RDS PostgreSQL database using IAM Auth."""
    schema_info = get_schema_info(host, port, user, region, db_name, logger)
    return list(schema_info.keys())

@mcp.tool()
def describe_table(host: str, port: int, user: str, region: str, db_name: str, table_name: str) -> list[dict]:
    """Get the column definitions for a specific table in the database."""
    schema_info = get_schema_info(host, port, user, region, db_name, logger)
    if table_name not in schema_info:
        return [{"error": f"Table '{table_name}' does not exist in database '{db_name}'."}]
    return schema_info[table_name]

@mcp.tool()
def run_read_query(host: str, port: int, user: str, region: str, db_name: str, query: str) -> list[dict]:
    """
    Run a read-only SQL SELECT query against the specified AWS RDS PostgreSQL database safely. 
    It leverages IAM authentication.
    """
    # Enforce basic SELECT only
    if not query.strip().upper().startswith("SELECT"):
        return [{"error": "MCP Tool restriction: Only SELECT queries are permitted."}]
        
    logger.info(f"Executing read query on '{db_name}': {query}")
    
    token = generate_iam_token(host, port, user, region, logger)
    encoded_token = quote_plus(token)
    db_url = f"postgresql+psycopg2://{user}:{encoded_token}@{host}:{port}/{db_name}"
    
    engine = create_engine(db_url, connect_args={'sslmode': 'require'})
    
    try:
        with engine.connect() as conn:
            result = conn.execute(sqlalchemy.text(query))
            columns = result.keys()
            rows = result.fetchall()
            return [{col: str(val) for col, val in zip(columns, row)} for row in rows]
    except Exception as e:
        logger.error(f"Failed to execute read query: {str(e)}")
        return [{"error": str(e)}]

if __name__ == "__main__":
    logger.info("Initialized PostgreSQL MCP Server.")
    # FastMCP run() executes the stdio server event loop natively
    mcp.run()
