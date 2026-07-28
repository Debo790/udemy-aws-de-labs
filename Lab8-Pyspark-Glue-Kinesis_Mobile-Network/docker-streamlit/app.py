import streamlit as st
import pandas as pd
import boto3
import time
from botocore.config import Config

# Configuration
REGION_NAME = 'eu-west-1'
ATHENA_DATABASE = 'presentation_lab'
S3_OUTPUT = 's3://udemy-aws-dataeng-labs/query_output/'

# Athena table names
TABLES = {
    'customer_entity': {
        'columns': ['customerid', 'entitytype', 'event_count', 'partition_hour', 'window_start', 'window_end'],
        'query': """
            SELECT customerid, entitytype, event_count, partition_hour, window_start, window_end
            FROM "{database}"."customer_entity"
            ORDER BY CAST(window_start AS TIMESTAMP) DESC
            LIMIT 10
        """
    }
}

session = boto3.Session()
athena = session.client('athena', region_name=REGION_NAME, config=Config())


def query_athena(query):
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': ATHENA_DATABASE},
        ResultConfiguration={'OutputLocation': S3_OUTPUT}
    )
    query_id = response['QueryExecutionId']
    while True:
        stats = athena.get_query_execution(QueryExecutionId=query_id)
        status = stats['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(1)
    
    if status == 'SUCCEEDED':
        result = athena.get_query_results(QueryExecutionId=query_id)
        column_names = [col['Label'] for col in result['ResultSet']['ResultSetMetadata']['ColumnInfo']]
        data = [[val.get('VarCharValue') for val in row['Data']] for row in result['ResultSet']['Rows'][1:]]
        df = pd.DataFrame(data, columns=column_names)
        return df
    else:
        st.error(f"Query failed to run by returning code of {status}.")
        return pd.DataFrame()


def display_table(title, df):
    if not df.empty:
        st.subheader(title)
        st.dataframe(df)
    else:
        st.write(f"No data available for {title}.")


# Streamlit app layout
st.title('Entity Bookings Real-Time Dashboard')
st.write('Displaying the latest data from various metrics. Refreshes every 2 minutes.')

# Fetch and display data from all tables
for table, info in TABLES.items():
    query = info['query'].format(database=ATHENA_DATABASE)
    df = query_athena(query)
    df.columns = info['columns']  # Ensure correct column names
    display_table(table.replace('_', ' ').title(), df)


# Setup auto-refresh
def auto_refresh(refresh_interval):
    # Display a message and countdown
    st_autorefresh = st.empty()
    for remaining in range(refresh_interval, 0, -1):
        st_autorefresh.text(f"Next update in {remaining} seconds.")
        time.sleep(1)
    st_autorefresh.text("Refreshing data...")
    st.experimental_rerun()


# Specify the refresh interval (120 seconds = 2 minutes)
auto_refresh(120)