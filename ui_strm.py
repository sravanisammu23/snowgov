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

import plotly.express as px

image = Image.open('C:\\Users\\sravani.sammu\\Downloads\\image.png')

st.sidebar.image(image, caption=None, width=None, use_column_width=None, clamp=False, channels="RGB", output_format="auto")

snowflake_config = st.secrets["sf_usage_app"]

#connect to snowflake function

SNOWFLAKE_CONFIG = {

    "account": "ie30988.ap-southeast-1",

    "user": "snowgov",

    "password": "G@nesh2$3",

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

        "nav-link": {"font-size": "23px", "text-align": "left", "margin":"0px", "--hover-color": "#eee"},

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

        db_team_name = st.text_input('BUSINESS_UNIT :', key="db_team_name_input")

        db_sub_team_name = st.text_input('PROJECT_SPACE :', key="db_sub_team_name_input")

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

        schema_team_name = st.text_input('BUSINESS_UNIT :', st.session_state.get('db_team_name', ''), key="schema_team_name_input")

        schema_sub_team_name = st.text_input('PROJECT_SPACE :', st.session_state.get('db_sub_team_name', ''), key="schema_sub_team_name_input")

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

        "nav-link": {"font-size": "23px", "text-align": "left", "margin":"0px", "--hover-color": "#eee"},

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

        "nav-link": {"font-size": "25px", "text-align": "left", "margin":"0px", "--hover-color": "#eee"},

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

        "nav-link": {"font-size": "23px", "text-align": "left", "margin":"0px", "--hover-color": "#eee"},

        "nav-link-selected": {"background-color": "#0096FF"},

    }

    )

    if dont_choose == "Account Usage":

        monitor2()

    elif dont_choose == "Detail Metrics":

        monitor3()

def monitor3():

    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)

    # Streamlit UI

    # Sidebar with Date Range filter at the top
    st.sidebar.header("Filters")

    date_option = st.sidebar.selectbox('Select Date Range', ['1 day', '7 days', '28 days', '1 year'])

    # Determine start_date and end_date based on the date_option selected
    if date_option == '1 day':
        start_date = datetime.now() - timedelta(days=1)
    elif date_option == '7 days':
        start_date = datetime.now() - timedelta(days=7)
    elif date_option == '28 days':
        start_date = datetime.now() - timedelta(days=28)
    else:
        start_date = datetime.now() - timedelta(days=365)

    end_date = datetime.now()

    # Rest of your code

    # Now, you can place the environment, project, and subject area filters below the date filter.

    environment = st.sidebar.selectbox('ENVIRONMENT :', ['DEV', 'PROD', 'STAGE', 'TEST'])
    query_projects = f"""
        SELECT DISTINCT MAX(CASE WHEN tag_name = 'COST_CENTER' THEN tag_value END)
        FROM "SNOWFLAKE"."ACCOUNT_USAGE".TAG_REFERENCES
        WHERE LEFT(object_name, 3) = '{environment}' AND domain = 'WAREHOUSE'
        GROUP BY object_name;
    """







    projects = execute_query(conn, query_projects)

    project_selection = st.sidebar.selectbox('PROJECT :', [project[0] for project in projects])

    query_subjects = f"""

        SELECT distinct trim(tag_value)

        FROM "SNOWFLAKE"."ACCOUNT_USAGE".TAG_REFERENCES

        WHERE LEFT(object_name, 3) = '{environment}'

        AND domain = 'WAREHOUSE'

        AND tag_name = 'SUBJECT_AREA'

        AND object_name IN (

            SELECT DISTINCT tr.object_name

            FROM "SNOWFLAKE"."ACCOUNT_USAGE".TAG_REFERENCES tr

            WHERE tr.tag_name = 'COST_CENTER' AND tr.tag_value = '{project_selection}'

        );

    """

    # Execute the query

    subject_areas = execute_query(conn, query_subjects)

    # Convert the list of tuples to a list of strings and then to a set to remove duplicates

    unique_subject_areas = list(set([subject[0] for subject in subject_areas]))

    # Create the Streamlit dropdown

    subject_selection = st.sidebar.selectbox('SUBJECT AREA :', unique_subject_areas)

    graph_option = st.sidebar.selectbox('Graph Data:', ['Warehouses filtered by Environment', 'Warehouses filtered by Projects', 'Warehouses filtered by Subject Area'])

    # First Graph without the date range

    if graph_option == 'Warehouses filtered by Environment':

        query_credits = f"""

            SELECT warehouse_name, SUM(credits_used) as credits_used

            FROM "SNOWFLAKE"."ACCOUNT_USAGE".WAREHOUSE_METERING_HISTORY

            WHERE LEFT(warehouse_name, 3) = '{environment}'

            GROUP BY warehouse_name

            ORDER BY 2 DESC;

        """

    elif graph_option == 'Warehouses filtered by Projects':

        query_credits = f"""

            SELECT warehouse_name, SUM(credits_used) as credits_used

            FROM "SNOWFLAKE"."ACCOUNT_USAGE".WAREHOUSE_METERING_HISTORY wmh

            WHERE LEFT(warehouse_name, 3) = '{environment}'

            AND warehouse_name IN (

                SELECT DISTINCT tr.object_name

                FROM "SNOWFLAKE"."ACCOUNT_USAGE".TAG_REFERENCES tr

                WHERE tr.tag_name = 'COST_CENTER' AND tr.tag_value = '{project_selection}'

            )

            GROUP BY warehouse_name

            ORDER BY 2 DESC;

        """

    else:  # Warehouses filtered by Subject Area

        query_credits = f"""

            SELECT warehouse_name, SUM(credits_used) as credits_used

            FROM "SNOWFLAKE"."ACCOUNT_USAGE".WAREHOUSE_METERING_HISTORY wmh

            WHERE LEFT(warehouse_name, 3) = '{environment}'

            AND warehouse_name IN (

                SELECT DISTINCT tr.object_name

                FROM "SNOWFLAKE"."ACCOUNT_USAGE".TAG_REFERENCES tr

                WHERE tr.tag_name = 'SUBJECT_AREA' AND tr.tag_value = '{subject_selection}'

            )

            GROUP BY warehouse_name

            ORDER BY 2 DESC;

        """

    warehouse_credits = execute_query(conn, query_credits)

    df_credits = pd.DataFrame(warehouse_credits, columns=['Warehouse', 'Credits'])

    # Sort the DataFrame by 'Credits' in descending order

    df_credits = df_credits.sort_values(by='Credits', ascending=True)



# Select the top 5 rows
    top_5_warehouses = df_credits.head(5)

# Create the bar chart with the top 5 warehouses
    fig = px.bar(top_5_warehouses, y='Warehouse', x='Credits', title='Top 5 Warehouses by Credits', orientation='h')

# Customize the formatting of the values on the y-axis
    fig.update_yaxes(tickformat=".15f")

    st.plotly_chart(fig)




    # Set the date range string for SQL based on the determined start_date and end_date

    date_range_str = f"BETWEEN '{start_date.strftime('%Y-%m-%d')}' AND '{end_date.strftime('%Y-%m-%d')}'"

    # Second Graph with the date range

    if graph_option == 'Warehouses filtered by Environment':

        query_credits = f"""

            SELECT start_time::date as usage_date, warehouse_name, SUM(credits_used) as total_credits_used

            FROM "SNOWFLAKE"."ACCOUNT_USAGE".WAREHOUSE_METERING_HISTORY

            WHERE LEFT(warehouse_name, 3) = '{environment}' AND start_time {date_range_str}

            GROUP BY usage_date, warehouse_name

            ORDER BY warehouse_name, usage_date;

        """

    elif graph_option == 'Warehouses filtered by Projects':

        query_credits = f"""

            SELECT start_time::date as usage_date, warehouse_name, SUM(credits_used) as total_credits_used

            FROM "SNOWFLAKE"."ACCOUNT_USAGE".WAREHOUSE_METERING_HISTORY wmh

            WHERE LEFT(warehouse_name, 3) = '{environment}' AND start_time {date_range_str}

            AND warehouse_name IN (

                SELECT DISTINCT tr.object_name

                FROM "SNOWFLAKE"."ACCOUNT_USAGE".TAG_REFERENCES tr

                WHERE tr.tag_name = 'COST_CENTER' AND tr.tag_value = '{project_selection}'

            )

            GROUP BY usage_date, warehouse_name

            ORDER BY warehouse_name, usage_date;

        """

    else:  # Warehouses filtered by Subject Area

        query_credits = f"""

            SELECT start_time::date as usage_date, warehouse_name, SUM(credits_used) as total_credits_used

            FROM "SNOWFLAKE"."ACCOUNT_USAGE".WAREHOUSE_METERING_HISTORY wmh

            WHERE LEFT(warehouse_name, 3) = '{environment}' AND start_time {date_range_str}

            AND warehouse_name IN (

                SELECT DISTINCT tr.object_name

                FROM "SNOWFLAKE"."ACCOUNT_USAGE".TAG_REFERENCES tr

                WHERE tr.tag_name = 'SUBJECT_AREA' AND tr.tag_value = '{subject_selection}'

            )

            GROUP BY usage_date, warehouse_name

            ORDER BY warehouse_name, usage_date;

        """

    warehouse_credits = execute_query(conn, query_credits)

    df_credits = pd.DataFrame(warehouse_credits, columns=['Usage Date', 'Warehouse', 'Total Credits Used'])

    # Sort the DataFrame by 'Total Credits Used' in descending order

    df_credits = df_credits.sort_values(by='Total Credits Used', ascending=False)

    st.markdown("**Credit Usage Overtime**")

    # Select the top ten rows

    top_10_warehouses = df_credits.head(3)

    # Create the pie chart with the top ten warehouses

    fig = px.pie(top_10_warehouses, names='Warehouse', values='Total Credits Used', hole=0.4)  # `hole` parameter makes it a donut chart

    # Customize the formatting of the values inside the chart

    fig.update_traces(textinfo='percent+label')

    st.plotly_chart(fig)

    fig.update_layout(yaxis_tickformat=".18f")

    #third graph

    # ...

    # Fetch and display data based on the selection

    if graph_option == 'Warehouses filtered by Environment':

        variance_query = f"""

            SELECT

                WAREHOUSE_NAME,

                DATE(START_TIME) AS DATE,

                SUM(CREDITS_USED) AS CREDITS_USED,

                AVG(SUM(CREDITS_USED)) OVER (

                    PARTITION BY WAREHOUSE_NAME

                    ORDER BY DATE ROWS 7 PRECEDING

                ) AS CREDITS_USED_7_DAY_AVG,

                (TO_NUMERIC(SUM(CREDITS_USED)/CREDITS_USED_7_DAY_AVG*100,10,2)-100)::STRING || '%' AS VARIANCE_TO_7_DAY_AVERAGE

            FROM

                "SNOWFLAKE"."ACCOUNT_USAGE"."WAREHOUSE_METERING_HISTORY"

            WHERE

                LEFT(WAREHOUSE_NAME, 3) = '{environment}' AND START_TIME BETWEEN '{start_date.strftime('%Y-%m-%d')}' AND '{end_date.strftime('%Y-%m-%d')}'

            GROUP BY

                DATE,

                WAREHOUSE_NAME

            ORDER BY

                DATE DESC;

        """

    elif graph_option == 'Warehouses filtered by Projects':

        variance_query = f"""

            SELECT

                WAREHOUSE_NAME,

                DATE(START_TIME) AS DATE,

                SUM(CREDITS_USED) AS CREDITS_USED,

                AVG(SUM(CREDITS_USED)) OVER (

                    PARTITION BY WAREHOUSE_NAME

                    ORDER BY DATE ROWS 7 PRECEDING

                ) AS CREDITS_USED_7_DAY_AVG,

                (TO_NUMERIC(SUM(CREDITS_USED)/CREDITS_USED_7_DAY_AVG*100,10,2)-100)::STRING || '%' AS VARIANCE_TO_7_DAY_AVERAGE

            FROM

                "SNOWFLAKE"."ACCOUNT_USAGE"."WAREHOUSE_METERING_HISTORY"

            WHERE

                LEFT(WAREHOUSE_NAME, 3) = '{environment}' AND START_TIME BETWEEN '{start_date.strftime('%Y-%m-%d')}' AND '{end_date.strftime('%Y-%m-%d')}'

                AND WAREHOUSE_NAME IN (

                    SELECT DISTINCT tr.object_name

                    FROM "SNOWFLAKE"."ACCOUNT_USAGE".TAG_REFERENCES tr

                    WHERE tr.tag_name = 'COST_CENTER' AND tr.tag_value = '{project_selection}'

                )

            GROUP BY

                DATE,

                WAREHOUSE_NAME

            ORDER BY

                DATE DESC;

        """

    else:  # Warehouses filtered by Subject Area

        variance_query = f"""

            SELECT

                WAREHOUSE_NAME,

                DATE(START_TIME) AS DATE,

                SUM(CREDITS_USED) AS CREDITS_USED,

                AVG(SUM(CREDITS_USED)) OVER (

                    PARTITION BY WAREHOUSE_NAME

                    ORDER BY DATE ROWS 7 PRECEDING

                ) AS CREDITS_USED_7_DAY_AVG,

                (TO_NUMERIC(SUM(CREDITS_USED)/CREDITS_USED_7_DAY_AVG*100,10,2)-100)::STRING || '%' AS VARIANCE_TO_7_DAY_AVERAGE

            FROM

                "SNOWFLAKE"."ACCOUNT_USAGE"."WAREHOUSE_METERING_HISTORY"

            WHERE

                LEFT(WAREHOUSE_NAME, 3) = '{environment}' AND START_TIME BETWEEN '{start_date.strftime('%Y-%m-%d')}' AND '{end_date.strftime('%Y-%m-%d')}'

                AND WAREHOUSE_NAME IN (

                    SELECT DISTINCT tr.object_name

                    FROM "SNOWFLAKE"."ACCOUNT_USAGE".TAG_REFERENCES tr

                    WHERE tr.tag_name = 'SUBJECT_AREA' AND tr.tag_value = '{subject_selection}'

                )

                GROUP BY

                DATE,

                WAREHOUSE_NAME

            ORDER BY

                DATE DESC;

        """

    # Execute the variance query

    warehouse_variance_data = execute_query(conn, variance_query)

    df_variance = pd.DataFrame(warehouse_variance_data, columns=['Warehouse', 'Date', 'Credits Used', '7 Day Avg Credits', 'Variance to 7 Day Avg'])

    # Display the bar chart with variance data

    fig_variance = px.bar(df_variance, x='Date', y='Variance to 7 Day Avg', color='Warehouse', title='Variance to 7 Day Avg per Warehouse over Time')

    st.plotly_chart(fig_variance)

    fig.update_layout(yaxis_tickformat=".15f")

    # ... [Previous code]

    # Fourth Graph: Credits by hour of the day

    if graph_option == 'Warehouses filtered by Environment':

        query_credits_hour = f"""

            SELECT to_char(start_time,'HH24') as hour, sum(credits_used)

            FROM snowflake.account_usage.warehouse_metering_history wmh

            WHERE wmh.start_time >= dateadd(month, -1, current_date()) AND LEFT(warehouse_name, 3) = '{environment}'

            GROUP BY to_char(start_time,'HH24')

            ORDER BY 1;

        """

    elif graph_option == 'Warehouses filtered by Projects':

        query_credits_hour = f"""

            SELECT to_char(start_time,'HH24') as hour, sum(credits_used)

            FROM snowflake.account_usage.warehouse_metering_history wmh

            WHERE wmh.start_time >= dateadd(month, -1, current_date()) AND LEFT(warehouse_name, 3) = '{environment}'

            AND warehouse_name IN (

                SELECT DISTINCT tr.object_name

                FROM "SNOWFLAKE"."ACCOUNT_USAGE".TAG_REFERENCES tr

                WHERE tr.tag_name = 'COST_CENTER' AND tr.tag_value = '{project_selection}'

            )

            GROUP BY to_char(start_time,'HH24')

            ORDER BY 1;

        """

    else:  # Warehouses filtered by Subject Area

        query_credits_hour = f"""

            SELECT to_char(start_time,'HH24') as hour, sum(credits_used)

            FROM snowflake.account_usage.warehouse_metering_history wmh

            WHERE wmh.start_time >= dateadd(month, -1, current_date()) AND LEFT(warehouse_name, 3) = '{environment}'

            AND warehouse_name IN (

                SELECT DISTINCT tr.object_name

                FROM "SNOWFLAKE"."ACCOUNT_USAGE".TAG_REFERENCES tr

                WHERE tr.tag_name = 'SUBJECT_AREA' AND tr.tag_value = '{subject_selection}'

            )

            GROUP BY to_char(start_time,'HH24')

            ORDER BY 1;

        """

    # Execute the credits by hour query

    hourly_credits_data = execute_query(conn, query_credits_hour)

    df_hourly_credits = pd.DataFrame(hourly_credits_data, columns=['Hour', 'Credits Used'])

    # Display the bar chart with hourly credits data

    fig_hourly_credits = px.line(df_hourly_credits, x='Hour', y='Credits Used', title='Credits Used by Hour of the Day')

    st.plotly_chart(fig_hourly_credits)

    # ... [Rest of the code]

    # Filter condition based on the graph option

    if graph_option == 'Warehouses filtered by Environment':

        filter_condition = f"LEFT(warehouse_name, 3) = '{environment}'"

    elif graph_option == 'Warehouses filtered by Projects':

        filter_condition = f"""

            LEFT(warehouse_name, 3) = '{environment}'

            AND warehouse_name IN (

                SELECT DISTINCT tr.object_name

                FROM "SNOWFLAKE"."ACCOUNT_USAGE".TAG_REFERENCES tr

                WHERE tr.tag_name = 'COST_CENTER' AND tr.tag_value = '{project_selection}'

            )

        """

    else:  # Warehouses filtered by Subject Area

        filter_condition = f"""

            LEFT(warehouse_name, 3) = '{environment}'

            AND warehouse_name IN (

                SELECT DISTINCT tr.object_name

                FROM "SNOWFLAKE"."ACCOUNT_USAGE".TAG_REFERENCES tr

                WHERE tr.tag_name = 'SUBJECT_AREA' AND tr.tag_value = '{subject_selection}'

            )

        """

    query_hourly_credits = f"""

        SELECT to_char(start_time,'HH24') as hour, sum(credits_used)

        FROM snowflake.account_usage.warehouse_metering_history wmh

        WHERE wmh.start_time >= dateadd(month, -1, current_date())

        AND {filter_condition}

        GROUP BY to_char(start_time,'HH24')

        ORDER BY 1;

    """

    hourly_credits_data = execute_query(conn, query_hourly_credits)

    df_hourly_credits = pd.DataFrame(hourly_credits_data, columns=['Hour', 'Credits Used'])

    hourly_credits_data = execute_query(conn, query_hourly_credits)

    df_hourly_credits = pd.DataFrame(hourly_credits_data, columns=['Hour', 'Credits Used'])

    # Create a Line Chart using plotly

    fig = px.line(df_hourly_credits, x='Hour', y='Credits Used', title="Credits Used in Each Hour of the Day")

    # Visualize the data using Streamlit

    st.plotly_chart(fig)

    # Fifth Graph: Average Execution Time per Type of Query

    query3 = f"""

        SELECT

            query_type,

            warehouse_size,

            AVG(execution_time) / 1000 as average_execution_time

        FROM

            snowflake.account_usage.query_history

        WHERE {filter_condition}

        GROUP BY

            1, 2

        ORDER BY

            3 DESC;

    """

    df3 = pd.DataFrame(execute_query(conn, query3))

    # Execute the query

    df3 = pd.DataFrame(execute_query(conn, query3), columns=['Query Type', 'Warehouse Size', 'Average Execution Time'])

    # Display the title in bold

    st.markdown("**Average Execution Time per Type of Query**")

    # Create the pie chart with the average execution time per type of query

    fig3 = px.pie(df3, names='Query Type', values='Average Execution Time', hole=0.4)  # `hole` parameter makes it a donut chart

    # Customize the formatting of the values inside the chart

    fig3.update_traces(textinfo='percent+label')

    st.plotly_chart(fig3)

    # Modify the SQL query to include the filter condition

    query_credits_by_service = f"""

        SELECT

            warehouse_name,

            SUM(credits_used_cloud_services) credits_used_cloud_services,

            SUM(credits_used_compute) credits_used_compute,

            SUM(credits_used) credits_used

        FROM

            snowflake.account_usage.warehouse_metering_history

        WHERE {filter_condition}

        GROUP BY

            1

        ORDER BY

            2 DESC

        LIMIT

            10;

    """

    # Execute the modified query

    credits_by_service_data = execute_query(conn, query_credits_by_service)

    # Convert the fetched data into a pandas DataFrame

    df_credits_by_service = pd.DataFrame(credits_by_service_data, columns=['Warehouse Name', 'Credits Used by Cloud Services', 'Credits Used by Compute', 'Total Credits Used'])

    # Create a bar graph using plotly

    fig_credits_by_service = px.bar(df_credits_by_service, x='Warehouse Name', y='Total Credits Used', title='Credits used by Cloud Services + compute by Warehouse')

    # Display the bar graph in Streamlit

    st.plotly_chart(fig_credits_by_service)

    # Top 10 users wrt credits used

    query = f"""

        SELECT user_name, COUNT(*),

            SUM(total_elapsed_time/1000 *

            CASE warehouse_size

                WHEN 'X-Small' THEN 1/60/60

                WHEN 'Small' THEN 2/60/60

                WHEN 'Medium' THEN 4/60/60

                WHEN 'Large' THEN 8/60/60

                WHEN 'X-Large' THEN 16/60/60

                WHEN '2X-Large' THEN 32/60/60

                WHEN '3X-Large' THEN 64/60/60

                WHEN '4X-Large' THEN 128/60/60

                ELSE 0

            END) AS estimated_credits

        FROM snowflake.account_usage.query_history

        WHERE {filter_condition}

        GROUP BY user_name

        ORDER BY 3 DESC

        LIMIT 10;

    """

    # Execute the query

    user_credits_data = execute_query(conn, query)

    df_user_credits = pd.DataFrame(user_credits_data, columns=['User Name', 'Count', 'Estimated Credits'])

    # Display the bar chart with the top 10 users with respect to credits used

    fig_user_credits = px.bar(df_user_credits, x='User Name', y='Estimated Credits', title='Top 10 Users by Estimated Credits')

    st.plotly_chart(fig_user_credits)

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

def Menu_navigator():

    with st.sidebar:

        choice = option_menu(

           menu_title=None,

            options=["USER","DATABASE" ,"ROLE", "MONITOR"],

            icons=[ "people-fill","database-fill", "person-lines-fill", "tv-fill"],

            menu_icon="snow2",

    styles={

        "container": {"padding": "0!important", "background-color": "#fafafa"},

        "nav-link": {"font-size": "23px", "text-align": "left", "margin":"0px", "--hover-color": "#eee"},

        "nav-link-selected": {"background-color": "#0096FF"},

             }

        )

    pages = {

        "User Creation": user_creation_page,

        "Database Management": database_management,

        "Role Management" : role_manage,

        "Monitor" : monitor

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