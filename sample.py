import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px

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

def execute_query(conn, query):
    cur = conn.cursor()
    cur.execute(query)
    result = cur.fetchall()
    cur.close()
    return result

def get_all_environments(conn):
    query = "SELECT DISTINCT LEFT(object_name, 3) FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES WHERE domain = 'WAREHOUSE';"
    return [row[0] for row in execute_query(conn, query)]

def get_projects_by_environments(conn, environments):
    formatted_environments = ', '.join(f"'{env}'" for env in environments)
    query = f"""
        SELECT DISTINCT MAX(CASE WHEN tag_name = 'COST_CENTER' THEN tag_value END)
        FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES
        WHERE LEFT(object_name, 3) IN ({formatted_environments})
        AND domain = 'WAREHOUSE'
        GROUP BY object_name;
    """
    return [row[0] for row in execute_query(conn, query)]

def get_subject_areas_by_projects(conn, projects):
    formatted_projects = ', '.join(f"'{proj}'" for proj in projects)
    query = f"""
        SELECT DISTINCT tag_value
        FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES
        WHERE tag_name = 'SUBJECT_AREA'
        AND domain = 'WAREHOUSE'
        AND object_name IN (
            SELECT DISTINCT object_name
            FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES
            WHERE tag_name = 'COST_CENTER' AND tag_value IN ({formatted_projects})
        );
    """
    return [row[0] for row in execute_query(conn, query)]

def monitor3():
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    st.sidebar.header("Filters")

    all_environments = get_all_environments(conn)
    selected_environments = st.sidebar.multiselect('ENVIRONMENT :', all_environments)

    related_projects = get_projects_by_environments(conn, selected_environments) if selected_environments else get_all_projects(conn)
    selected_projects = st.sidebar.multiselect('PROJECT :', related_projects)

    related_subject_areas = get_subject_areas_by_projects(conn, selected_projects) if selected_projects else get_all_subject_areas(conn)
    selected_subject_areas = st.sidebar.multiselect('SUBJECT AREA :', related_subject_areas)

    # construct your conditions and SQL query here based on the selections and display results

    conn.close()

# define get_all_projects and get_all_subject_areas functions to get all available projects and subject areas from the tags.
def get_all_projects(conn):
    query = """
        SELECT DISTINCT MAX(CASE WHEN tag_name = 'COST_CENTER' THEN tag_value END)
        FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES
        WHERE domain = 'WAREHOUSE'
        GROUP BY object_name;
    """
    return [row[0] for row in execute_query(conn, query)]

def get_all_subject_areas(conn):
    query = """
        SELECT DISTINCT tag_value
        FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES
        WHERE tag_name = 'SUBJECT_AREA' AND domain = 'WAREHOUSE';
    """
    return [row[0] for row in execute_query(conn, query)]

if __name__ == "__main__":
    monitor3()
