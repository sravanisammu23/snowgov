import streamlit as st
import pandas as pd
import snowflake.connector
import snowflake.connector.errors  # <-- Corrected import
from streamlit_option_menu import option_menu
import matplotlib.pyplot as plt
import numpy as np
import plost
from datetime import datetime, timedelta
from utils import charts, gui, processing
from utils import snowflake_connector as sf
from utils import sql as sql
from PIL import Image
import base64
import plotly.express as px
image = Image.open('C:\\Users\\sravani.sammu\\Downloads\\image.png')
st.sidebar.image(image, caption=None, width=None, use_column_width=None, clamp=False, channels="RGB", output_format="auto")
snowflake_config = st.secrets["sf_usage_app"]
#connect to snowflake function
SNOWFLAKE_CONFIG = {
    "account": "pr65711.ap-southeast-1",
    "user": "snowgovernance",
    "password": "Sravani@23",
    "role": "accountadmin",
    "warehouse": "COMPUTE_WH",
    "database": "UTIL_DB",
    "schema": "ADMIN_TOOLS"
}
if "grant_users" not in st.session_state:
    st.session_state.grant_users = []
def apply_css_styles():
    # Set the background color of the main page
    st.markdown("<style>body {background-color: #3498DB;}</style>", unsafe_allow_html=True)
    # Set the background color of the Streamlit navigation bar
    st.markdown("<style>.sidebar .sidebar-content {background-color: #2980B9; color: white;}</style>", unsafe_allow_html=True)
def connect_to_snowflake(conn_params):
    return snowflake.connector.connect(**conn_params)  # <-- Corrected line
def snowflake_connection():
    st.markdown("<style>.reportview-container .main .block-container {background-color: #DCEEFB;}</style>", unsafe_allow_html=True)
    #st.title("LOGIN:")
    account_name = st.text_input("Name this Connection:")
    account_url = st.text_input("Account URL:")
    username = st.text_input("Username:")
    password = st.text_input("Password:", type="password")
    account_parts = account_url.split('.') if account_url else []
    snowflake_account = account_parts[0] if len(account_parts) > 0 else ""
    region = account_parts[1] if len(account_parts) > 1 else ""
    conn_params = {
        "user": username,
        "password": password,
        "account": snowflake_account,
        "region": region,
    }
    if st.button("Connect"):
        try:
            conn = connect_to_snowflake(conn_params)
            if conn:
                st.session_state.conn = conn
                st.session_state.connections[account_name] = conn_params
                st.success(f"🔗 Connected to {account_name}!")
            else:
                st.error("Unable to establish a connection.")
        except snowflake.connector.errors.DatabaseError as e:  # <-- Corrected exception
            st.error(f"🚫 Connection failed. Error: {e}")
            st.markdown("</div>", unsafe_allow_html=True)
def create_database_and_schema(conn, environment, team_name, sub_team_name):
    cursor = conn.cursor()
    result_messages = []
    try:
        procedure_name_db = "UTIL_DB.ADMIN_TOOLS.SETUP_DATA_MART"
        call_query_db = f"CALL {procedure_name_db}(%s, %s, %s)"
        cursor.execute(call_query_db, (environment, team_name, sub_team_name))
        result_messages=cursor.fetchall()
        return result_messages[0][0]
    except Exception as e:
        result_messages.append(f"Error creating database: {e}")
    cursor.close()
    return "\n".join(result_messages)
def set_role(conn, role_name="ACCOUNTADMIN"):
    """Set the role for the current Snowflake session."""
    cursor = conn.cursor()
    try:
        cursor.execute(f"USE ROLE {role_name};")
    except Exception as e:
        st.error(f"Error setting role: {e}")
    finally:
        cursor.close()
def create_schema(conn, environment, team_name, sub_team_name, schema_name, power_user_privilege, analyst_privilege, data_engineer_privilege):
    cursor = conn.cursor()
    result_messages = []  # Initialize result_messages
    try:
        procedure_name = "UTIL_DB.ADMIN_TOOLS.SETUP_SCHEMA_V3"
        call_query = f"CALL {procedure_name}(%s, %s, %s, %s, %s, %s, %s)"
        cursor.execute(call_query, (environment, team_name, sub_team_name, schema_name, power_user_privilege, analyst_privilege, data_engineer_privilege))
        result_messages = cursor.fetchall()
        return result_messages[0][0]
    except Exception as e:
        result_messages.append(f"Error creating schema: {e}")
        cursor.close()
        return "\n".join(result_messages)
def database_management():
    choose = option_menu(
        menu_title="PROJECT SPACE",
        options=["Database", "Schema"],
        icons=["database-fill-add", "database-fill-lock"],
        orientation="horizontal",
        menu_icon="database-fill-gear",
         styles={
        "container": {"padding": "0!important", "background-color": "#fafafa"},
        "nav-link": {"font-family":"Sans serif","font-size": "18px", "text-align": "left", "margin":"0px", "--hover-color": "#eee"},
        "nav-link-selected": {"background-color": "#0096FF"},
    }
    )
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    # Database Section
    environment = ''
    db_team_name = ''
    db_sub_team_name = ''
    if choose == 'Database':
        #st.write("Create DataBase")
        environment = st.selectbox('ENVIRONMENT :', ['DEV', 'PROD', 'STAGE', 'TEST'])
        db_team_name = st.text_input('BUSINESS UNIT :', key="db_team_name_input")
        db_sub_team_name = st.text_input('PROJECT :', key="db_sub_team_name_input")
        # Store the values in session_state
        st.session_state.environment = environment
        st.session_state.db_team_name = db_team_name
        st.session_state.db_sub_team_name = db_sub_team_name
        if st.button('SETUP'):
            set_role(conn, "ACCOUNTADMIN")
            message = create_database_and_schema(conn, environment, db_team_name, db_sub_team_name)
            st.write(message)
    if choose == 'Schema':
        #st.write("Create Schema")
        # Retrieve the values from session_state to pre-populate the input fields
        schema_name = st.text_input("SCHEMA NAME :", key="schema_name_input")
        schema_env = st.text_input("ENVIRONMENT :", st.session_state.get('environment', ''), key="schema_env_input")
        schema_team_name = st.text_input('BUSINESS UNIT :', st.session_state.get('db_team_name', ''), key="schema_team_name_input")
        schema_sub_team_name = st.text_input('PROJECT :', st.session_state.get('db_sub_team_name', ''), key="schema_sub_team_name_input")
        # Using st.expander for Privilege Assignment
        with st.expander("PRIVILEGE ASSIGNMENT"):
            privilege_options = ["Read Only", "Read/Write", "Full Access"]
            power_user_privilege = st.selectbox("POWER USER", privilege_options, index=2)
            analyst_privilege = st.selectbox("ANALYST", privilege_options, index=1)
            data_engineer_privilege = st.selectbox("DATA ENGINEER", privilege_options, index=0)
            if power_user_privilege == "Read Only" and analyst_privilege == "Full Access":
                st.write("Invalid combination: POWER USER cannot have lower privileges than ANALYST.")
        if st.button('Create'):
            set_role(conn, "ACCOUNTADMIN")
            message = create_schema(conn, schema_env, schema_team_name, schema_sub_team_name, schema_name,
                                    power_user_privilege, analyst_privilege, data_engineer_privilege)
            st.write(message)
def create_snowflake_user(user_name, f_name, l_name, email):
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    procedure_name = "call UTIL_DB.STREAMLIT_TOOLS.CREATE_USER ( '"+user_name+"','"+f_name+"','"+l_name+"','"+email+"')"
    result = cursor.execute(procedure_name).fetchall()
        # Check if the result contains the username, indicating successful creation
          # Return the result as it is
    cursor.close()
    return result[0][0]
def user_creation_page():
    not_required = option_menu(
        menu_title = "USER CREATION",
        options = ["User"],
        icons=['person-bounding-box'],
        menu_icon ='person-fill-add',
         styles={
        "container": {"padding": "0!important", "background-color": "#fafafa"},
        "nav-link": {"font-family":"Sans serif","font-size": "18px", "text-align": "left", "margin":"0px", "--hover-color": "#eee"},
        "nav-link-selected": {"background-color": "#0096FF"},
    }
    )
    user_name = st.text_input("USERNAME :")
    f_name = st.text_input("FIRST NAME :")
    l_name = st.text_input("LAST NAME :")
    email = st.text_input("EMAIL :")
    if st.button("Create"):
        result = create_snowflake_user(user_name, f_name, l_name, email)
        st.write(result)  # This will display "User already exists!" if the user already exists
def role_manage():
    role_choice = option_menu(
        menu_title = "Role Management",
        options = ["Role Assign","List Users","Revoke Role"],
        icons = ["person-check","person-video2","person-fill-slash"],
        orientation = 'horizontal',
        menu_icon = 'person-fill-gear',
         styles={
        "container": {"padding": "0!important", "background-color": "#fafafa"},
        "nav-link": {"font-family":"Sans serif","font-size": "18px", "text-align": "left", "margin":"0px", "--hover-color": "#eee"},
        "nav-link-selected": {"background-color": "#0096FF"},
    }
    )
    if role_choice == 'Role Assign':
        role_assignment()
    if role_choice == 'List Users':
        role_list()
    if role_choice == 'Revoke Role':
        revoke_role()
def fetch_roles_for_user3(username):
    con = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    grants_query = f'SHOW GRANTS TO USER {username};'
    cursor = con.cursor().execute(grants_query)
    result = cursor.fetchall()
    con.close()
     # Updated table data to include 'Grantee', 'Grantee Name', and 'Role' columns.
    table_data = [
        {
            'Grantee': row[2],
            'Grantee Name': row[3],
            'Role Name': row[1]
        }
        for row in result
    ]
    return table_data
def revoke_roles_and_log_using_sp3(username, roles_to_revoke):
    con = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    roles_array_str = ', '.join([f"'{role}'" for role in roles_to_revoke])
    call_sp_query = f'CALL "BILLING_USAGE"."DASHBOARD"."REVOKE_USER_GRANTS_AND_LOG"(\'{username}\', ARRAY_CONSTRUCT({roles_array_str}))'
    result = con.cursor().execute(call_sp_query).fetchone()
    con.close()
    return result[0]
def revoke_role():
    con = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    users = [row[0] for row in con.cursor().execute('SHOW USERS;').fetchall()]
    con.close()
    selected_user = st.selectbox('Select User', users)
    roles_table_data = fetch_roles_for_user3(selected_user)
    if not roles_table_data:
        st.warning(f'No roles assigned to {selected_user} yet.')
    else:
        with st.expander("Roles already assigned"):
            st.table(roles_table_data)
    roles_to_revoke = st.multiselect('Select Roles to Revoke', [row['Role Name'] for row in roles_table_data])
    if st.button('Revoke Roles'):
        if not roles_to_revoke:
            st.warning('Please select roles to revoke.')
        else:
            result_message = revoke_roles_and_log_using_sp3(selected_user, roles_to_revoke)
            st.write(result_message)
def connect_to_snowflake2():
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cursor = conn.cursor()
        return conn, cursor
    except Exception as e:
        st.error(f"Connection Error: {str(e)}")
        return None, None
# Function to connect to Snowflake
def connect_to_snowflake2():
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        return conn
    except Exception as e:
        st.write(f"Error connecting to Snowflake: {e}")
        return None
# Function to fetch roles from Snowflake
def fetch_roles2(conn):
    try:
        cur = conn.cursor()
        cur.execute('SELECT ROLE_NAME FROM "BILLING_USAGE"."DASHBOARD"."SPECIFIC_ROLES"')
        roles = [row[0] for row in cur.fetchall()]
        cur.close()
        return roles
    except Exception as e:
        st.write(f"Error fetching roles: {e}")
        return []
# Function to fetch users for a given role from Snowflake
def fetch_users_for_role2(conn, role):
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT DISTINCT UserName FROM \"BILLING_USAGE\".\"DASHBOARD\".\"USER_ROLE_MAPPING\" WHERE ROLENAME= '{role}'")
        users = [row[0] for row in cur.fetchall()]
        cur.close()
        return users
    except Exception as e:
        st.write(f"Error fetching users for role {role}: {e}")
        return []
def role_list():
    #st.title("Fetch Users for Role")
  conn = connect_to_snowflake2()
  if not conn:
        return
    # Fetch roles
  roles = fetch_roles2(conn)
  if not roles:
    st.write("No roles fetched. Exiting.")
    conn.close()
    return
    # Role selection
  chosen_role = st.selectbox('Select a Role', roles)
    # Fetch users for the selected role
  users = fetch_users_for_role2(conn, chosen_role)
    # Display users in a table without index
  if users:
        st.write(f"Users associated with Role '{chosen_role}':")
        user_df = pd.DataFrame({"User": users})
        st.dataframe(user_df)
  else:
        st.write(f"No users found for the role: {chosen_role}")
  conn.close()
def execute_query(conn,query):
    cur = conn.cursor()
    cur.execute(query)
    result = cur.fetchall()
    cur.close()
    return result
def fetch_roles_for_user(username):
    con = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    grants_query = f'SHOW GRANTS TO USER {username};'
    cursor = con.cursor().execute(grants_query)
    result = cursor.fetchall()
    con.close()
    # Table data to include 'Grantee', 'Grantee Name', and 'Role' columns.
    table_data = [
        {
            'Grantee': row[2],
            'Grantee Name': row[3],
            'Role Name': row[1]
        }
        for row in result
    ]
    return table_data
def grant_roles_and_log_using_sp(username, roles_to_grant):
    con = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    roles_array_str = ', '.join([f"'{role}'" for role in roles_to_grant])
    call_sp_query = f'CALL "BILLING_USAGE"."DASHBOARD"."GRANT_USER_ROLES_AND_LOG"(\'{username}\', ARRAY_CONSTRUCT({roles_array_str}))'
    result = con.cursor().execute(call_sp_query).fetchone()
    con.close()
    return result[0]
def fetch_all_roles():
    con = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    sp_query = 'SELECT role_name FROM "BILLING_USAGE"."DASHBOARD"."SPECIFIC_ROLES";'
    all_roles = [row[0] for row in con.cursor().execute(sp_query).fetchall()]
    con.close()
    return all_roles
def role_assignment():
    # Connect to Snowflake
    con = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    users = [row[0] for row in con.cursor().execute('SHOW USERS;').fetchall()]
    con.close()
    selected_user = st.selectbox('Select User', users)
    granted_roles_data = fetch_roles_for_user(selected_user)
    granted_roles = [row['Role Name'] for row in granted_roles_data]
    if not granted_roles_data:
        st.warning(f'No roles assigned to {selected_user} yet.')
    else:
        with st.expander("Roles already assigned"):
            st.table(granted_roles_data)
    # Fetch all roles and filter out the roles already granted
    all_roles = fetch_all_roles()
    roles_to_display = list(set(all_roles) - set(granted_roles))
    roles_to_grant = st.multiselect('Select Roles to Grant', roles_to_display)
    if st.button('Assign Roles'):
        if not roles_to_grant:
            st.warning('Please select roles to grant.')
        else:
            result_message = grant_roles_and_log_using_sp(selected_user, roles_to_grant)
            st.write(result_message)
def monitor():
    dont_choose = option_menu(
        menu_title="CREDITS USAGE",
        options=["Account Usage", "Detail Metrics"],
        icons=["display-fill", "display-fill"],
        orientation="horizontal",
         styles={
        "container": {"padding": "0!important", "background-color": "#fafafa"},
        "nav-link": {"font-family":"Sans serif","font-size": "18px", "text-align": "left", "margin":"0px", "--hover-color": "#eee"},
        "nav-link-selected": {"background-color": "#0096FF"},
    }
    )
    if dont_choose == "Account Usage":
        monitor2()
    elif dont_choose == "Detail Metrics":
        monitor3()
def execute_query(conn, query):
    cur = conn.cursor()
    try:
        cur.execute(query)
        results = cur.fetchall()
    except Exception as e:
        st.error(f"Error executing query: {e}")
        results = []
    finally:
        cur.close()
    return results
def construct_project_query(environments):
    environments = [env for env in environments if env != 'All']
    if environments:
        environments_str = ', '.join(f"'{env}'" for env in environments)
        return f"""
            SELECT DISTINCT MAX(CASE WHEN tag_name = 'COST_CENTER' THEN tag_value END)
            FROM "SNOWFLAKE"."ACCOUNT_USAGE".TAG_REFERENCES
            WHERE LEFT(object_name, 3) IN ({environments_str}) AND domain = 'WAREHOUSE'
            GROUP BY object_name;
        """
    else:
        return """
            SELECT DISTINCT MAX(CASE WHEN tag_name = 'COST_CENTER' THEN tag_value END)
            FROM "SNOWFLAKE"."ACCOUNT_USAGE".TAG_REFERENCES
            WHERE domain = 'WAREHOUSE'
            GROUP BY object_name;
        """
def construct_subject_query(environments, projects):
    environments = [env for env in environments if env != 'All']
    projects = [proj for proj in projects if proj != 'All']
    environments_str = ', '.join(f"'{env}'" for env in environments) if environments else ''
    projects_str = ', '.join(f"'{proj}'" for proj in projects) if projects else ''
    if environments and projects:
        return f"""
            SELECT DISTINCT trim(tag_value)
            FROM "SNOWFLAKE"."ACCOUNT_USAGE".TAG_REFERENCES
            WHERE LEFT(object_name, 3) IN ({environments_str}) AND domain = 'WAREHOUSE' AND tag_name = 'SUBJECT_AREA'
            AND object_name IN (
                SELECT DISTINCT tr.object_name
                FROM "SNOWFLAKE"."ACCOUNT_USAGE".TAG_REFERENCES tr
                WHERE tr.tag_name = 'COST_CENTER' AND tr.tag_value IN ({projects_str})
            );
        """
    elif environments:
        return f"""
            SELECT DISTINCT tag_value
            FROM "SNOWFLAKE"."ACCOUNT_USAGE".TAG_REFERENCES
            WHERE LEFT(object_name, 3) IN ({environments_str}) AND domain = 'WAREHOUSE' AND tag_name = 'SUBJECT_AREA';
        """
    elif projects:
        return f"""
            SELECT DISTINCT tag_value
            FROM "SNOWFLAKE"."ACCOUNT_USAGE".TAG_REFERENCES
            WHERE domain = 'WAREHOUSE' AND tag_name = 'SUBJECT_AREA'
            AND object_name IN (
                SELECT DISTINCT tr.object_name
                FROM "SNOWFLAKE"."ACCOUNT_USAGE".TAG_REFERENCES tr
                WHERE tr.tag_name = 'COST_CENTER' AND tr.tag_value IN ({projects_str})
            );
        """
    else:
        return """
            SELECT DISTINCT trim(tag_value)
            FROM "SNOWFLAKE"."ACCOUNT_USAGE".TAG_REFERENCES
            WHERE domain = 'WAREHOUSE' AND tag_name = 'SUBJECT_AREA';
        """
def construct_query(environments, projects, subject_areas, start_date, end_date):
    # Condition for single item in each list and it's not 'All'
    if (len(environments) == 1 and environments[0] != 'All' and
        len(projects) == 1 and projects[0] != 'All' and
        len(subject_areas) == 1 and subject_areas[0] != 'All'):
        environment = environments[0]
        subject_selection = subject_areas[0]
        return f"""
            SELECT warehouse_name, SUM(credits_used) as credits_used
            FROM "SNOWFLAKE"."ACCOUNT_USAGE".WAREHOUSE_METERING_HISTORY
            WHERE LEFT(warehouse_name, 3) = '{environment}'
            AND warehouse_name IN (
                SELECT DISTINCT tr.object_name
                FROM "SNOWFLAKE"."ACCOUNT_USAGE".TAG_REFERENCES tr
                WHERE tr.tag_name = 'SUBJECT_AREA' AND tr.tag_value = '{subject_selection}'
            )
            AND start_time >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'
            AND start_time <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'
            GROUP BY warehouse_name
            ORDER BY 2 DESC;
        """
    else:
        environments = [env for env in environments if env != 'All']
        projects = [proj for proj in projects if proj != 'All']
        subject_areas = [subj for subj in subject_areas if subj != 'All']
        environments_str = ', '.join(f"'{env}'" for env in environments) if environments else ''
        projects_str = ', '.join(f"'{proj}'" for proj in projects) if projects else ''
        subject_areas_str = ', '.join(f"'{subj}'" for subj in subject_areas) if subject_areas else ''
        where_clauses = []
        if environments:
            where_clauses.append(f"LEFT(warehouse_name, 3) IN ({environments_str})")
        if projects:
            where_clauses.append(f"warehouse_name IN (SELECT DISTINCT tr.object_name FROM \"SNOWFLAKE\".\"ACCOUNT_USAGE\".TAG_REFERENCES tr WHERE tr.tag_name = 'COST_CENTER' AND tr.tag_value IN ({projects_str}))")
        if subject_areas:
            where_clauses.append(f"warehouse_name IN (SELECT DISTINCT trim(tr.object_name) FROM \"SNOWFLAKE\".\"ACCOUNT_USAGE\".TAG_REFERENCES tr WHERE tr.tag_name = 'SUBJECT_AREA' AND tr.tag_value IN ({subject_areas_str}))")
        where_clause = ' AND '.join(where_clauses) if where_clauses else "1=1"  # default to true if there are no where clauses
        return f"""
            SELECT warehouse_name, SUM(credits_used) as credits_used
            FROM "SNOWFLAKE"."ACCOUNT_USAGE".WAREHOUSE_METERING_HISTORY
            WHERE {where_clause}
            AND start_time >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'
            AND start_time <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'
            GROUP BY warehouse_name
            ORDER BY 2 DESC;
        """
def fetch_all_warehouses(conn, environment, project, subject_area):
    query = f"""
        SELECT DISTINCT wmh.warehouse_name
        FROM "SNOWFLAKE"."ACCOUNT_USAGE".WAREHOUSE_METERING_HISTORY wmh
        JOIN "SNOWFLAKE"."ACCOUNT_USAGE".TAG_REFERENCES tr
        ON wmh.warehouse_name = tr.object_name
        WHERE LEFT(wmh.warehouse_name, 3) = '{environment}'
        AND tr.tag_name = 'COST_CENTER' AND tr.tag_value = '{project}'
        AND tr.tag_name = 'SUBJECT_AREA' AND tr.tag_value = '{subject_area}'
    """
    cur = conn.cursor()
    cur.execute(query)
    results = cur.fetchall()
    cur.close()
    return [result[0] for result in results]
def construct_hourly_query(environment, start_date):
    return f"""
        SELECT to_char(start_time, 'HH24') as hour, sum(credits_used)
        FROM "SNOWFLAKE"."ACCOUNT_USAGE".WAREHOUSE_METERING_HISTORY
        WHERE start_time >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'
        AND LEFT(warehouse_name, 3) = '{environment}'
        GROUP BY hour
        ORDER BY hour;
    """
def display_hourly_credits_chart(conn, environments, start_date):
    # Initialize an empty DataFrame to aggregate hourly credits
    combined_hourly_credits = pd.DataFrame(columns=['Hour', 'Credits'])
    # Iterate through selected environments and aggregate hourly credits
    for env in environments:
        query_hourly_credits = construct_hourly_query(env, start_date)
        hourly_credits = execute_query(conn, query_hourly_credits)
        if hourly_credits:
            df_hourly_credits = pd.DataFrame(hourly_credits, columns=['Hour', 'Credits'])
            combined_hourly_credits = pd.concat([combined_hourly_credits, df_hourly_credits], ignore_index=True)
    if not combined_hourly_credits.empty:
        # Create a single graph for all environments combined
        fig_hourly = px.line(combined_hourly_credits, x='Hour', y='Credits', title='Credits Used Per Hour')
        st.plotly_chart(fig_hourly)
    else:
        st.warning("No hourly data available for the selected environment(s).")
def display_bar_graph(data):
    if not data:
        st.warning("No data available for the selected filters.")
    else:
        df = pd.DataFrame(data, columns=['Query Type', 'Warehouse Size', 'Average Execution Time'])
        fig = px.bar(
            df,
            x='Query Type',
            y='Average Execution Time',
            color='Warehouse Size',
            title='Average Execution Time by Query Type and Warehouse Size'
        )
        st.plotly_chart(fig)
def monitor3():
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    st.sidebar.header("Filters")
    # Date Range Filter
    date_option = st.sidebar.selectbox('Select Date Range', ['1 day', '7 days', '28 days', '1 year'])
    if date_option == '1 day':
        start_date = datetime.now() - timedelta(days=1)
    elif date_option == '7 days':
        start_date = datetime.now() - timedelta(days=7)
    elif date_option == '28 days':
        start_date = datetime.now() - timedelta(days=28)
    else:  # 1 year
        start_date = datetime.now() - timedelta(days=365)
    end_date = datetime.now()
    # Environment Filter
    environments = ['All', 'DEV', 'PRO', 'STA', 'TES']
    selected_environments = st.sidebar.multiselect('ENVIRONMENT :', environments, default=['All'])
    if not selected_environments:
        st.warning("Please select at least one option for the Environment filter.")
        return
    if selected_environments:
        projects = ['All'] + [result[0].strip() if result[0] is not None else '' for result in execute_query(conn, construct_project_query(selected_environments))]
        selected_projects = st.sidebar.multiselect('PROJECT :', projects, default=['All'])
    # Subject Area Filter
        subject_areas = ['All'] + [result[0].strip() for result in execute_query(conn, construct_subject_query(selected_environments, selected_projects))]
        selected_subject_areas = st.sidebar.multiselect('SUBJECT AREA :', subject_areas, default=['All'])
        # Constructing Query based on Graph Option
        query_credits = construct_query(selected_environments, selected_projects, selected_subject_areas, start_date, end_date)
        warehouse_credits = execute_query(conn, query_credits)
        if not warehouse_credits:
            st.warning("No data available for the selected filters.")
        else:
            df_credits = pd.DataFrame(warehouse_credits, columns=['Warehouse', 'Credits'])
            df_credits = df_credits.sort_values(by='Credits', ascending=True)  # Sort in ascending order by credits
            top_5_warehouses = df_credits.tail(5)  # Select the last 5 warehouses (top 5 in ascending order)
            fig = px.bar(top_5_warehouses, y='Warehouse', x='Credits', title='Top 5 Warehouses by Credits', orientation='h')
            fig.update_yaxes(tickformat=".15f")
            st.plotly_chart(fig)
# ...
    # Existing logic to construct the query and execute it
    query_credits = construct_query(selected_environments, selected_projects, selected_subject_areas, start_date, end_date)
    warehouse_credits = execute_query(conn, query_credits)
    if not warehouse_credits:
        st.warning("No data available for the selected filters.")
    else:
        df_credits = pd.DataFrame(warehouse_credits, columns=['Warehouse', 'Credits'])
        df_credits = df_credits.sort_values(by='Credits', ascending=False)  # Sort in descending order by credits
        top_5_warehouses = df_credits.head(5)  # Select the top 5 warehouses (top 5 in descending order)
        # Creating a pie chart for the top 5 warehouses by credits
        fig = px.pie(top_5_warehouses, names='Warehouse', values='Credits', title='Top 5 Warehouses by Credits')
        # Displaying the pie chart
        st.plotly_chart(fig)
    # Hourly Credits
    if 'All' in selected_environments:
        display_hourly_credits_chart(conn, environments[1:], start_date)  # Exclude 'All' from selected environments
    else:
        display_hourly_credits_chart(conn, selected_environments, start_date)
        # Total Credits
    if 'All' in selected_environments:
        selected_env_str = ', '.join([f"'{env[:3]}'" for env in environments[1:]])
    else:
        selected_env_str = ', '.join([f"'{env[:3]}'" for env in selected_environments])
#queries
    if 'All' in selected_environments:
            filter_condition = "1=1"  # Default condition when 'All' is selected
    else:
            selected_env_str = ', '.join([f"'{env[:3]}'" for env in selected_environments])
            filter_condition = f"LEFT(warehouse_name, 3) IN ({selected_env_str})"
        # Construct the query based on the filter condition
    query = f"""
            SELECT
                query_type,
                warehouse_size,
                AVG(execution_time) / 1000 as average_execution_time
            FROM
                snowflake.account_usage.query_history
            WHERE
                {filter_condition}
            GROUP BY
                1, 2
            ORDER BY
                3 DESC;
        """
        # Execute the query
    result_data = execute_query(conn, query)
        # Display the results as a bar graph
    display_bar_graph(result_data)
    # Define a function to fetch and display the top 10 users by estimated credits used as a bar graph
    def display_top_10_users(conn):
        query_top_10_users = """
            SELECT user_name,
                count(*) as query_count,
                sum(total_elapsed_time/1000 *
                case warehouse_size
                when 'X-Small' then 1/60/60
                when 'Small'   then 2/60/60
                when 'Medium'  then 4/60/60
                when 'Large'   then 8/60/60
                when 'X-Large' then 16/60/60
                when '2X-Large' then 32/60/60
                when '3X-Large' then 64/60/60
                when '4X-Large' then 128/60/60
                else 0
                end) as estimated_credits
            FROM snowflake.account_usage.query_history
            GROUP BY user_name
            ORDER BY estimated_credits DESC
            LIMIT 10;
        """
        top_10_users_data = execute_query(conn, query_top_10_users)
        if not top_10_users_data:
            st.warning("No data available for the top users.")
        else:
            df_top_10_users = pd.DataFrame(top_10_users_data, columns=['User Name', 'Query Count', 'Estimated Credits'])
            # Create a bar chart to display the top 10 users
            fig_top_10_users = px.bar(df_top_10_users, x='User Name', y='Estimated Credits', title='Top 10 Users by Estimated Credits Used')
            st.plotly_chart(fig_top_10_users)
    # Call the function to display the top 10 users as a bar graph
    display_top_10_users(conn)
    # Define a function to fetch and display top 5 warehouse performance by query type
    def display_top_5_warehouse_performance_by_query_type(conn, selected_environments):
        # Construct the SQL query based on the selected environments
        if 'All' in selected_environments:
            query_performance_by_query_type = """
                SELECT
                    warehouse_name,
                    query_type,
                    AVG(execution_time) AS avg_execution_time_seconds
                FROM
                    snowflake.account_usage.query_history
                GROUP BY
                    warehouse_name, query_type
                ORDER BY
                    warehouse_name, avg_execution_time_seconds DESC
                LIMIT 5;
            """
        else:
            selected_env_str = ', '.join([f"'{env[:3]}'" for env in selected_environments])
            query_performance_by_query_type = f"""
                SELECT
                    warehouse_name,
                    query_type,
                    AVG(execution_time) AS avg_execution_time_seconds
                FROM
                    snowflake.account_usage.query_history
                WHERE
                    LEFT(warehouse_name, 3) IN ({selected_env_str})
                GROUP BY
                    warehouse_name, query_type
                ORDER BY
                    warehouse_name, avg_execution_time_seconds DESC
                LIMIT 5;
            """
        performance_by_query_type_data = execute_query(conn, query_performance_by_query_type)
        if not performance_by_query_type_data:
            st.warning("No data available for top 5 warehouse performance by query type.")
        else:
            df_performance_by_query_type = pd.DataFrame(performance_by_query_type_data, columns=[
                'Warehouse Name', 'Query Type', 'Average Execution Time (seconds)'
            ])
        df_performance_by_query_type = df_performance_by_query_type.sort_values(by='Average Execution Time (seconds)', ascending=False)
        fig_performance_by_query_type = px.bar(df_performance_by_query_type,
                                        x='Warehouse Name',
                                        y='Average Execution Time (seconds)',
                                        color='Query Type',
                                        title='Top 5 Warehouse Performance by Query Type',
                                        category_orders={"Warehouse Name": df_performance_by_query_type['Warehouse Name'].tolist()})
            # Create a bar chart to display top 5 warehouse performance by query type
        st.plotly_chart(fig_performance_by_query_type)
    # ... (your existing code)
    display_top_5_warehouse_performance_by_query_type(conn, selected_environments)
    # Define a function to fetch and display top 5 credits used by Cloud Services and Compute by Warehouse
    def display_top_5_credits_by_warehouse(conn, selected_environments):
        if 'All' in selected_environments:
            query_credits_by_warehouse = """
                SELECT
                    warehouse_name,
                    SUM(credits_used_cloud_services) AS credits_used_cloud_services,
                    SUM(credits_used_compute) AS credits_used_compute,
                    SUM(credits_used) AS credits_used
                FROM
                    snowflake.account_usage.warehouse_metering_history
                GROUP BY
                    1
                ORDER BY
                    credits_used DESC
                LIMIT
                    5;  -- Limit to top 5
            """
        else:
            selected_env_str = ', '.join([f"'{env}'" for env in selected_environments])
            query_credits_by_warehouse = f"""
                SELECT
                    warehouse_name,
                    SUM(credits_used_cloud_services) AS credits_used_cloud_services,
                    SUM(credits_used_compute) AS credits_used_compute,
                    SUM(credits_used) AS credits_used
                FROM
                    snowflake.account_usage.warehouse_metering_history
                WHERE
                    LEFT(warehouse_name, 3) IN ({selected_env_str})
                GROUP BY
                    1
                ORDER BY
                    credits_used DESC
                LIMIT
                    5;  -- Limit to top 5
            """
        credits_by_warehouse_data = execute_query(conn, query_credits_by_warehouse)
        if not credits_by_warehouse_data:
            st.warning("No data available for credits used by warehouse.")
        else:
            df_credits_by_warehouse = pd.DataFrame(credits_by_warehouse_data, columns=[
                'Warehouse Name', 'Credits Used by Cloud Services', 'Credits Used by Compute', 'Total Credits Used'
            ])
            # Create a bar chart to display credits used by Cloud Services and Compute by Warehouse
            fig_credits_by_warehouse = px.histogram(df_credits_by_warehouse, x='Warehouse Name', y=[
                'Credits Used by Cloud Services', 'Credits Used by Compute', 'Total Credits Used'
            ], title='Top 5 Warehouses by Credits Used for Cloud Services and Compute')
            # Rotate the y-axis labels for better readability
            fig_credits_by_warehouse.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig_credits_by_warehouse)
    # Call the function to display top 5 credits used by Cloud Services and Compute by Warehouse
    display_top_5_credits_by_warehouse(conn, selected_environments)
    # Define the SQL query as a constant
    CREDITS_BY_WAREHOUSE_QUERY = """
        SELECT
            warehouse_name,
            SUM(credits_used_cloud_services) AS credits_used_cloud_services,
            SUM(credits_used_compute) AS credits_used_compute,
            SUM(credits_used) AS credits_used
        FROM
            snowflake.account_usage.warehouse_metering_history
        GROUP BY
            1
        ORDER BY
            credits_used DESC
        LIMIT
            5;  -- Limit to top 5
    """
    conn.close()
def monitor2():
    with st.sidebar:
        date_from, date_to = gui.date_selector()
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    #conn.cursor().execute(query)
    gui.space(1)
    st.subheader("WAREHOUSE METERING")
    query = sql.CONSUMPTION_PER_SERVICE_TYPE_QUERY
    df = sf.sql_to_dataframe(
        query.format(date_from=date_from, date_to=date_to)
    )
    all_values = df["SERVICE_TYPE"].unique().tolist()
    selected_value = "All"
    if selected_value == "All":
        selected_value = all_values
    else:
        selected_value = [selected_value]
    df = df[df["SERVICE_TYPE"].isin(selected_value)]
    consumption = int(df["CREDITS_USED"].sum())
    if df.empty:
        st.caption("No data found.")
    elif consumption == 0:
        st.caption("No consumption found.")
    else:
        # Sum of credits used
        credits_used_html = gui.underline(
            text=gui.pretty_print_credits(consumption),
        )
        credits_used_html += " were used"
        gui.space(1)
        st.write(credits_used_html, unsafe_allow_html=True)
        gui.space(1)
        gui.subsubheader(
            "**Compute** spend over time",
            "Aggregated by day",
        )
        # Resample by day
        df_resampled = processing.resample_by_day(
            df,
            date_column="START_TIME",
        )
        bar_chart = charts.get_bar_chart(
            df=df_resampled,
            date_column="START_TIME",
            value_column="CREDITS_USED",
        )
        st.altair_chart(bar_chart, use_container_width=True)
        # Group by
        agg_config = {"CREDITS_USED": "sum"}
        df_grouped = (
            df.groupby(["NAME", "SERVICE_TYPE"]).agg(agg_config).reset_index()
        )
        df_grouped_top_10 = df_grouped.sort_values(
            by="CREDITS_USED", ascending=False
        ).head(10)
        df_grouped_top_10["CREDITS_USED"] = df_grouped_top_10[
            "CREDITS_USED"
        ].apply(gui.pretty_print_credits)
        gui.subsubheader(
            "**Compute** spend",
            " Grouped by NAME",
            "Top 10",
        )
        st.dataframe(
            gui.dataframe_with_podium(
                df_grouped_top_10,
            )[["NAME", "SERVICE_TYPE", "CREDITS_USED"]],
            width=600,
        )
        gui.space(1)
        gui.hbar()
        st.subheader("Warehouse")
    # Get data
    warehouse_usage_hourly = sf.sql_to_dataframe(
        sql.WAREHOUSE_USAGE_HOURLY.format(
            date_from=date_from,
            date_to=date_to,
        )
    )
    # Add filtering widget per Warehouse name
    warehouses = warehouse_usage_hourly.WAREHOUSE_NAME.unique()
    selected_warehouse = st.selectbox(
        "Choose warehouse",
        warehouses.tolist(),
         key="warehouse_selector_1"
    )
    # Filter accordingly
    warehouse_usage_hourly_filtered = warehouse_usage_hourly[
        warehouse_usage_hourly.WAREHOUSE_NAME.eq(selected_warehouse)
    ]
    # Resample so that all the period has data (fill with 0 if needed)
    warehouse_usage_hourly_filtered = processing.resample_date_period(
        warehouse_usage_hourly_filtered,
        date_from,
        date_to,
        value_column="CREDITS_USED_COMPUTE",
    )
    gui.subsubheader("Time-histogram of **warehouse usage**")
    plost.time_hist(
        data=warehouse_usage_hourly_filtered,
        date="START_TIME",
        x_unit="day",
        y_unit="hours",
        color={
            "field": "CREDITS_USED_COMPUTE",
            "scale": {
                "scheme": charts.ALTAIR_SCHEME,
            },
        },
        aggregate=None,
        legend=None,
    )
    gui.space(1)
    gui.hbar()
    # -----------------
    # ---- Queries ----
    # -----------------
    st.subheader("Queries")
    # Get data
    queries_data = sf.get_queries_data(
        date_from,
        date_to,
    )
    # Add filtering widget per Warehouse name
    warehouses = queries_data.WAREHOUSE_NAME.dropna().unique().tolist()
    selected_warehouse = st.selectbox(
        "Choose warehouse",
        warehouses,
         key="warehouse_selector_2"
    )
    # Filter accordingly
    queries_data = queries_data[
        queries_data.WAREHOUSE_NAME.eq(selected_warehouse)
    ]
    gui.subsubheader(
        "Histogram of **queries duration** (in secs)", "Log scale"
    )
    # Histogram
    histogram = charts.get_histogram_chart(
        df=queries_data,
        date_column="DURATION_SECS",
    )
    st.altair_chart(histogram, use_container_width=True)
    # Top-3 longest queries
    queries_podium_df = gui.dataframe_with_podium(
        queries_data, "DURATION_SECS"
    ).head(3)
    # Only show if at least three queries!
    if len(queries_podium_df) >= 3:
        with st.expander("🔎 Zoom into top-3 longest queries in detail"):
            for query in queries_podium_df.itertuples():
                st.caption(f"{query.Index} {query.DURATION_SECS_PP}")
                st.code(query.QUERY_TEXT_PP, "sql")
    gui.space(1)
    st.write("Time-histograms of **aggregate queries duration** (in secs)")
    # Resample so that all the period has data (fill with 0 if needed)
    queries_data = processing.resample_date_period(
        queries_data, date_from, date_to, "DURATION_SECS"
    )
    num_days_selected = (date_to - date_from).days
    if num_days_selected > 14:
        st.caption("Week-day histogram")
        plost.time_hist(
            data=queries_data,
            date="START_TIME",
            x_unit="week",
            y_unit="day",
            color={
                "field": "DURATION_SECS",
                "scale": {"scheme": charts.ALTAIR_SCHEME},
            },
            aggregate="sum",
            legend=None,
        )
    st.caption("Day-hour histogram")
    plost.time_hist(
        data=queries_data,
        date="START_TIME",
        x_unit="day",
        y_unit="hours",
        color={
            "field": "DURATION_SECS",
            "scale": {"scheme": charts.ALTAIR_SCHEME},
        },
        aggregate="sum",
        legend=None,
    )
    gui.space(1)
    gui.subsubheader(
        "**Query VS Time Frequency**: longest and most frequent queries",
        "Log scales (🖱️ hover for real values!)",
    )
    queries_agg = sf.sql_to_dataframe(
        sql.QUERIES_COUNT_QUERY.format(
            date_from=date_from,
            date_to=date_to,
            num_min=1,
            limit=10_000,
            warehouse_name=selected_warehouse,
        )
    )
    queries_agg = processing.apply_log1p(
        df=queries_agg, columns=["EXECUTION_MINUTES", "NUMBER_OF_QUERIES"]
    )
    scatter_chart = charts.get_scatter_chart(df=queries_agg)
    st.altair_chart(
        scatter_chart,
        use_container_width=True,
    )
    gui.space(1)
    gui.hbar()
    # -------------
    # --- Users ---
    # -------------
    st.subheader("Users")
    # Get data
    users_data = sf.sql_to_dataframe(
        sql.USERS_QUERY.format(
            date_from=date_from,
            date_to=date_to,
        )
    )
    gui.subsubheader("**Users** with the **largest** number of credits spent")
    # Bar chart
    plost.bar_chart(
        data=users_data,
        bar="USER_NAME",
        value="APPROXIMATE_CREDITS_USED",
        color=gui.BLUE_COLOR,
        direction="horizontal",
        height=200,
        use_container_width=True,
    )
def about():
    # Create an expander for the about section
    with st.expander("About", expanded=True):
        # Load and display the image with adjusted width
        image_path = 'C:\\Users\\sravani.sammu\\Downloads\\image.png'
        image = Image.open(image_path)
        st.image(image, caption=None, width=300, use_column_width=None, clamp=False, channels="RGB", output_format="auto")
        # Write the about content with styling
        st.markdown("""
            <div style="font-family: 'Sans-serif';">
                <p>This project is to demonstrate the power of Snowflake Native Apps. The objective of this project is to develop an App that provides GUI-based governance features for managing the Snowflake environment. Some of the features include:</p>
                <ul>
                    <li>User interface through which the IT team can configure Organization and Account Parameters</li>
                    <li>User Interface through which IT teams can create Projects (a logical entity). For each Project, they can create multiple Environments (Dev, Stage, Production). Internally for each environment, the app creates Databases or schemas depending on configuration. For each project and environment, provide a GUI to create warehouses</li>
                    <li>Onboard users to projects and assign respective roles on each environment (i.e. Database or Schemas)</li>
                    <li>Provide Cost-monitoring dashboards drilled down by Accounts, Projects, Environments, Users, etc.</li>
                </ul>
            </div>
            """, unsafe_allow_html=True)
def Menu_navigator():
    with st.sidebar:
        choice = option_menu(
           menu_title="MENU",
            options=["USER","DATABASE" ,"ROLE", "MONITOR","ABOUT"],
            icons=["people-fill","database-fill", "person-lines-fill", "tv-fill","info-circle-fill"],
            menu_icon="snow2",
    styles={
        "container": {"padding": "0!important", "background-color": "#fafafa"},
        "nav-link": {"font-family":"Sans serif","font-size": "18px", "text-align": "left", "margin":"0px", "--hover-color": "#eee"},
        "nav-link-selected": {"background-color": "#0096FF"},
             }
        )
    pages = {
        "User Creation": user_creation_page,
        "Database Management": database_management,
        "Role Management" : role_manage,
        "Monitor" : monitor,
        "ABOUT"   : about
    }
    current_page = st.session_state.get("current_page", "User Creation")
    if choice == 'DATABASE':
        current_page = "Database Management"
    elif choice == 'USER':
        current_page = "User Creation"
    elif choice == 'ROLE':
        current_page = "Role Management"
    elif choice == 'MONITOR':
        current_page = "Monitor"
    elif choice == 'ABOUT':
        current_page = "ABOUT"
    # Add more elif conditions if you have more choices/pages
    st.session_state.current_page = current_page
    pages[current_page]()
    st.sidebar.markdown("</div>", unsafe_allow_html=True)
# Main function with CSS applied at the beginning
def main():
    st.markdown("<style>body {background-color: #3498DB;}</style>", unsafe_allow_html=True)
    st.markdown("<style>.stButton>button {background-color: #2980B9; color: white;}</style>", unsafe_allow_html=True)
    if "conn" not in st.session_state:
        st.session_state.conn = None
    if "connections" not in st.session_state:
        st.session_state.connections = {}
    Menu_navigator()
if __name__ == "__main__":
    main()