import streamlit as st
from datetime import datetime, timedelta
import pandas as pd
import plotly.express as px
import snowflake.connector

# Snowflake Configuration
SNOWFLAKE_CONFIG = {
    "account": "xh84085.ap-southeast-1",
    "user": "sravani12",
    "password": "Sravani@12",
    "role": "accountadmin",
    "warehouse": "COMPUTE_WH",
    "database": "UTIL_DB",
    "schema": "ADMIN_TOOLS"
}

def execute_query(conn, query, params=None):
    try:
        cur = conn.cursor()
        if params:
            cur.execute(query, params)
        else:
            cur.execute(query)
        result = cur.fetchall()
        cur.close()
        return result
    except Exception as e:
        st.write(f"Error executing query: {e}")
        return []

def monitor3():
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)

    # Streamlit UI
    st.sidebar.header("Filters")

    # Date Range Filter
    date_option = st.sidebar.selectbox('Select Date Range', ['1 day', '7 days', '28 days', '1 year'])
    if date_option == '1 day':
        start_date = datetime.now() - timedelta(days=1)
    elif date_option == '7 days':
        start_date = datetime.now() - timedelta(days=7)
    elif date_option == '28 days':
        start_date = datetime.now() - timedelta(days=28)
    else:
        start_date = datetime.now() - timedelta(days=365)
    end_date = datetime.now()

    # Environment Multi-Select
    environments = st.sidebar.multiselect('ENVIRONMENT :', ['DEV', 'PROD', 'STAGE', 'TEST'])

    # Fetch projects based on selected environments
    projects = []
    for env in environments:
        query_projects = f"""
            SELECT DISTINCT MAX(CASE WHEN tag_name = 'COST_CENTER' THEN tag_value END)
            FROM "SNOWFLAKE"."ACCOUNT_USAGE".TAG_REFERENCES
            WHERE LEFT(object_name, 3) = '{env}' AND domain = 'WAREHOUSE'
            GROUP BY object_name;
        """
        projects.extend(execute_query(conn, query_projects))
    unique_projects = list(set([project[0] for project in projects]))
    selected_projects = st.sidebar.multiselect('PROJECT :', unique_projects)

    # Fetch subject areas based on selected environments and projects
    subject_areas = []
    for env in environments:
        for proj in selected_projects:
            query_subjects = f"""
                SELECT distinct trim(tag_value)
                FROM "SNOWFLAKE"."ACCOUNT_USAGE".TAG_REFERENCES
                WHERE LEFT(object_name, 3) = '{env}'
                AND domain = 'WAREHOUSE'
                AND tag_name = 'SUBJECT_AREA'
                AND object_name IN (
                    SELECT DISTINCT tr.object_name
                    FROM "SNOWFLAKE"."ACCOUNT_USAGE".TAG_REFERENCES tr
                    WHERE tr.tag_name = 'COST_CENTER' AND tr.tag_value = '{proj}'
                );
            """
            subject_areas.extend(execute_query(conn, query_subjects))
    unique_subject_areas = list(set([subject[0] for subject in subject_areas]))
    selected_subject_areas = st.sidebar.multiselect('SUBJECT AREA :', unique_subject_areas)

    # Construct the SQL query based on the selections
    query_conditions = []
    if environments:
        env_str = "', '".join(environments)
        query_conditions.append(f"LEFT(warehouse_name, 3) IN ('{env_str}')")
    if selected_projects:
        proj_str = "', '".join(selected_projects)
        query_conditions.append(f"warehouse_name IN (SELECT DISTINCT tr.object_name FROM \"SNOWFLAKE\".\"ACCOUNT_USAGE\".TAG_REFERENCES tr WHERE tr.tag_name = 'COST_CENTER' AND tr.tag_value IN ('{proj_str}'))")
    if selected_subject_areas:
        subj_str = "', '".join(selected_subject_areas)
        query_conditions.append(f"warehouse_name IN (SELECT DISTINCT tr.object_name FROM \"SNOWFLAKE\".\"ACCOUNT_USAGE\".TAG_REFERENCES tr WHERE tr.tag_name = 'SUBJECT_AREA' AND tr.tag_value IN ('{subj_str}'))")

    query_credits = f"""
        SELECT warehouse_name, SUM(credits_used) as total_credits_used
        FROM "SNOWFLAKE"."ACCOUNT_USAGE".WAREHOUSE_METERING_HISTORY
        WHERE start_time BETWEEN '{start_date}' AND '{end_date}'
        AND {' AND '.join(query_conditions)}
        GROUP BY warehouse_name
        ORDER BY total_credits_used DESC
        LIMIT 5;
    """

    # Execute the query and display results
    top_warehouses = execute_query(conn, query_credits)
    df_credits = pd.DataFrame(top_warehouses, columns=['Warehouse', 'Total Credits Used'])

    # Check if the dataframe is empty
    if df_credits.empty:
        st.write("No data available for the selected filters.")
    else:
        fig = px.bar(df_credits, x='Warehouse', y='Total Credits Used', title='Top 5 Warehouses by Credits')
        st.plotly_chart(fig)

if __name__ == "__main__":
    monitor3()
