import pandas as pd
from sqlalchemy import create_engine
from django.conf import settings

# Database connection string (you can store this in Django settings if you prefer)
db_url = "postgresql://default:w2KGfbOSHy6R@ep-twilight-sunset-a45mqvb6.us-east-1.aws.neon.tech:5432/verceldb?sslmode=require"

# Create a SQLAlchemy engine
engine = create_engine(db_url)

# Function to execute a query and return results as a DataFrame
def query_to_dataframe(query, params=None):
    try:
        # Execute query and fetch data into a DataFrame
        with engine.connect() as connection:
            df = pd.read_sql(query, connection, params=params)
        return df
    except Exception as e:
        print(f"Error executing query: {e}")
        return None

# Example usage: Function to fetch all rows from the 'opusnet_table' as a DataFrame
def fetch_all_opusnet_data():
    query = "SELECT * FROM opusnet_table"
    return query_to_dataframe(query)