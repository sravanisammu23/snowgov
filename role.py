import streamlit as st
import pandas as pd
import snowflake.connector
import snowflake.connector.errors
from streamlit_option_menu import option_menu
import matplotlib.pyplot as plt
import numpy as np
import plost
from datetime import datetime, timedelta
from utils import snowflake_connector as sf
from utils import sql as sql
from utils import charts,processing,gui
from PIL import Image
import base64
import plotly.express as px
#image = Image.open('image_1.png')
st.markdown("""
    <style>
        .main .block-container {
	
	    margin-top:  -0rem;
            margin-left: -0rem;  /* Adjust this value as needed to reduce the gap */
        }
    </style>
    """, unsafe_allow_html=True)


def get_custom_css():

    return """ <style>

.stButton>button {
    background: linear-gradient(to right, #a02a41 0%,    #1D4077 100%);
    font-family: Poppins;
    padding: 10px 30px;
    color: white;
    border-radius: 50px;
    margin-left: 0px
}
.sidebar .sidebar-content {
    background-color: red;  /* Change to your desired color for the sidebar background */
    color: red;  /* Change to your desired color for the font in the sidebar */
}
</style>"""
custom_css = get_custom_css()
st.markdown(custom_css, unsafe_allow_html=True)
#st.sidebar.image(image, caption=None, width=None, use_column_width=None, clamp=False, channels="RGB", output_format="auto")
#snowflake_config = st.secrets["sf_usage_app"]
#connect to snowflake function
SNOWFLAKE_CONFIG = {
    "account": "fy50889.us-east4.gcp",#https://anblicksorg_aws.us-east-1.snowflakecomputing.com
    "user": "snowgov",#snowgov
    "password": "WelcomeToGCP23!",#SnowGov@202308
    "role": "SNOWGOVADMIN_ACL",
    "warehouse": "SNOWGOV_WAREHOUSE",#SNOWGOV_WH
    "database": "UTIL_DB",
    "schema": "ADMIN_TOOLS"
}
with open ('styles_1.css') as f:
        st.markdown(f'<style>{f.read()}</style>',unsafe_allow_html=True)

if "grant_users" not in st.session_state:
    st.session_state.grant_users = []
def apply_css_styles():
    # Set the background color of the main page
    st.markdown("<style>body {background-color: linear-gradient(90deg, #FFF -4.75%, rgba(255, 255, 255, 0.00) 78%),border-radius: 12px;}</style>", unsafe_allow_html=True)
    # Set the background color of the Streamlit navigation bar
    st.markdown("<style>body {background-color:linear-gradient(90deg, #FFF -4.75%, rgba(255, 255, 255, 0.00) 78%) ,border-radius: 12px;}</style>", unsafe_allow_html=True)
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
def fetch_environments_from_db(conn):
    cursor = conn.cursor()
    cursor.execute("select TEAM_NAME from BILLING_USAGE.DASHBOARD.TEAM")
    environments = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return environments
def fetch_sub_teams_from_db(conn):
    cursor = conn.cursor()
    cursor.execute("USE ROLE ACCOUNTADMIN")
    cursor.execute("select distinct(SUB_TEAM_NAME) from BILLING_USAGE.DASHBOARD.SUB_TEAM")
    sub_teams = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return sub_teams
def database_management():
    col1, col2 = st.columns([1,20])

    with col1:
        im = Image.open("Project-Title.png")
        st.image(im, width=24)

    with col2:
        st.markdown("<p style='color: black;font-family: Poppins;font-size: 18px;font-style: normal;font-weight: 600;line-height: normal;'>Project Space</p>", unsafe_allow_html=True)

    # Custom CSS to style the various elements
    st.markdown("""
                <style>
                    /* Styles for the tabs */
                    .stTabs [data-baseweb="tab-list"] {
                    gap: 20px;
                    }

                    .stTabs [data-baseweb="tab"] {
                    font-family: 'Poppins' !important;
                    }

                    /* Styles for widget labels and reducing the gap */
                    .stMarkdown p {
                        color: black !important;
                        font-family: Poppins;
                        margin-bottom: -1rem !important;
                    }

                    .stTextInput > div,
                    .stRadio > div,
                    .stSelectbox > div {
                        margin-top: -1rem !important;
                    }

                </style>""", unsafe_allow_html=True)

    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    available_teams = fetch_environments_from_db(conn)
    available_sub_teams = fetch_sub_teams_from_db(conn)

    tab1, tab2 = st.tabs(["**Database**", "**Schema**"])

    with tab1:
        with st.form(key='my_form2'):
            st.markdown("<p style='font-family: Poppins; color: black;'>Environment :</p>", unsafe_allow_html=True)
            environment = st.radio('', ['DEV', 'PROD', 'STAGE', 'TEST'])

            st.markdown("<p style='font-family: Poppins; color: black;'>Business Unit :</p>", unsafe_allow_html=True)
            db_team_name = st.selectbox('', available_teams)

            st.markdown("<p style='font-family: Poppins; color: black;'>Project :</p>", unsafe_allow_html=True)
            db_sub_team_name = st.text_input('', key="db_sub_team_name_input")

            submit_button_db = st.form_submit_button(label='Setup')

        if submit_button_db:
            if not environment or not db_team_name or not db_sub_team_name.strip():
                st.write("Please fill in all required fields for the Database setup.")
            else:
                set_role(conn, "ACCOUNTADMIN")
                message = create_database_and_schema(conn, environment, db_team_name, db_sub_team_name)
                st.write(message)

    with tab2:
        with st.form(key='my_form3'):
            st.markdown("<p style='font-family: Poppins; color: black;'>Schema Name :</p>", unsafe_allow_html=True)
            schema_name = st.text_input("", key="schema_name_input")

            st.markdown("<p style='font-family: Poppins; color: black;'>Environment :</p>", unsafe_allow_html=True)
            schema_env = st.selectbox('', ['DEV', 'PROD', 'STAGE', 'TEST'])

            st.markdown("<p style='font-family: Poppins; color: black;'>Business Unit :</p>", unsafe_allow_html=True)
            schema_team_name = st.selectbox('', available_teams)

            st.markdown("<p style='font-family: Poppins; color: black;'>Project :</p>", unsafe_allow_html=True)
            schema_sub_team_name = st.selectbox('', available_sub_teams)

            with st.expander("Privilege Assignment"):
                privilege_options = ["Read Only", "Read/Write", "Full Access"]
                power_user_placeholder = st.empty()
                power_user_privilege = st.selectbox("", privilege_options, index=2)
                power_user_placeholder.markdown("<p style='font-family: Poppins; color: black;'>Power User</p>", unsafe_allow_html=True)
                analyst_placeholder = st.empty()
                analyst_privilege = st.selectbox("", privilege_options, index=1)
                analyst_placeholder.markdown("<p style='font-family: Poppins; color: black;'>Analyst</p>", unsafe_allow_html=True)
                data_engineer_placeholder = st.empty()
                data_engineer_privilege = st.selectbox("", privilege_options, index=0)
                data_engineer_placeholder.markdown("<p style='font-family: Poppins; color: black;'>Data Engineer</p>", unsafe_allow_html=True)

            submit_button_schema = st.form_submit_button(label='Create Schema')

        if submit_button_schema:
            if not schema_name.strip() or not schema_env or not schema_team_name or not schema_sub_team_name:
                st.write("Please fill in all required fields for the Schema setup.")
            else:
                set_role(conn, "ACCOUNTADMIN")
                message = create_schema(conn, schema_env, schema_team_name, schema_sub_team_name, schema_name, power_user_privilege, analyst_privilege, data_engineer_privilege)
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
    col1, col2 = st.columns([1,20])

    with col1:
        im = Image.open("User-Title.png")
        st.image(im, width=24)

    with col2:
        st.markdown("<p style='color: black;font-family: Poppins;font-size: 18px;font-style: normal;font-weight: 600;line-height: normal;'>User Creation</p>", unsafe_allow_html=True)

    with st.form(key='my_form'):
        custom_css = get_custom_css()
        st.markdown(custom_css, unsafe_allow_html=True)

        # Custom CSS to reduce gaps
        st.markdown("""
                    <style>
                        .stMarkdown p {
                            margin-bottom: -0.9rem !important;  # Reducing the space after the markdown labels
                        }
                        .stTextInput > div {
                            margin-top: -0.9rem !important;    # Reducing the space before the widgets
                        }
                    </style>
                    """, unsafe_allow_html=True)

        st.markdown("<p style='font-family: Poppins; color: black;'>Username :</p>", unsafe_allow_html=True)
        user_name = st.text_input("", key="username")

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("<p style='font-family: Poppins; color: black;'>First Name :</p>", unsafe_allow_html=True)
            f_name = st.text_input("", key="first_name")
        with col2:
            st.markdown("<p style='font-family: Poppins; color: black;'>Last Name :</p>", unsafe_allow_html=True)
            l_name = st.text_input("", key="last_name")

        st.markdown("<p style='font-family: Poppins; color: black;'>Email :</p>", unsafe_allow_html=True)
        email = st.text_input("", key="email")

        submit_button = st.form_submit_button(label='Create')

    if submit_button:
        # Check if any of the fields are empty
        if not user_name or not f_name or not l_name or not email:
            st.write("Please enter a value for all required fields.")
        else:
            result = create_snowflake_user(user_name, l_name, f_name, email)
            st.write(result) # This will display "User already exists!" if the user already exists


# user_creation_page()
def role_manage():

    col1, col2 = st.columns([1,20])

    with col1:
        im = Image.open("Role-Title.png")
        st.image(im,width=24)

    with col2:
        #st.write('Manage Role')
        st.markdown("<p style='color: black;font-family: Poppins;font-size: 18px;font-style: normal;font-weight: 600;line-height: normal;'>  Manage Role</p>", unsafe_allow_html=True)



    st.markdown("""
<style>

	.stTabs [data-baseweb="tab-list"] {
		gap: 20px;
    }

	.stTabs [data-baseweb="tab"] {
                color:grey;
font-family: Poppins;
font-size: 14px;
font-style: normal;
font-weight: 400;
line-height: normal;
    }

	.stTabs [aria-selected="true"] {
                color:red;
  		background-color: #FFFFFF;
	}

</style>""", unsafe_allow_html=True)
    tab1, tab2, tab3 = st.tabs(["**Role Assign**", "**List Users**", "**Revoke Role**"])

    with tab1:
        role_assignment()

    with tab2:
        role_list()

    with tab3:
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
    # Connect to Snowflake
    con = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    result_message = None

    # Create columns layout
    col1, col2, col3 = st.columns([33, 33, 34])

    # Fetch users and display the user selectbox
    users = [row[0] for row in con.cursor().execute('SELECT DISTINCT USERNAME FROM BILLING_USAGE.DASHBOARD.RESULT_TABLE;').fetchall()]

    with col1:
        st.markdown("""
    <style>
    .stSelectbox:first-of-type > div[data-baseweb="select"] > div {
              padding: 5px;
                width : 100%;
    }
                .stSelectbox>label {
                    width:400%;
                color: #A7ABC6;
font-family: Poppins;
font-size: 12px;
font-style: normal;
font-weight: 600;
line-height: normal;
    }
    </style>
    """, unsafe_allow_html=True)
        selected_user = st.selectbox('Select User', users, key='u1')

    roles_table_data = fetch_roles_for_user3(selected_user)

    # Display assigned roles in a table without index
    role_df = pd.DataFrame(roles_table_data)
    st.dataframe(role_df)

    # Display the roles to revoke multiselect widget
    with col2:
        st.markdown("""
    <style>
        .stMultiSelect [data-baseweb=select] span{
            padding: 5px;
            width : 100%;
            max-width: 250px;
            font-size: 0.6rem;
        }
                        .stMultiSelect>label {
                color: #CBCACA;
              padding: 5px;
                width : 100%;
    }
    </style>
    """, unsafe_allow_html=True)
        roles_to_revoke = st.multiselect('Select Roles to Revoke', [row['Role Name'] for row in roles_table_data])

    # Revoke roles button
    with col3:
        st.markdown(get_css_for_button(), unsafe_allow_html=True)
        if st.button('Revoke'):
            if not roles_to_revoke:
                st.warning('**Please select roles to revoke.**')
            else:
                result_message = revoke_roles_and_log_using_sp3(selected_user, roles_to_revoke)

    if result_message:
        st.write(result_message)

    con.close()



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
        cur.execute(f"SELECT DISTINCT USERNAME FROM \"BILLING_USAGE\".\"DASHBOARD\".\"RESULT_TABLE\" WHERE ROLENAME= '{role}'")
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
    st.markdown("""
    <style>
    .stSelectbox:first-of-type > div[data-baseweb="select"] > div {
              padding: 5px;
                width : 100%;
    }
                .stSelectbox>label {
                color: #CBCACA;
              padding: 5px;
                width : 25%;
    }
    </style>
""", unsafe_allow_html=True)

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
    result_message=None

    col1, col2,col3 = st.columns([33,33,34])
    users = [row[0] for row in con.cursor().execute('SELECT DISTINCT USERNAME FROM BILLING_USAGE.DASHBOARD.RESULT_TABLE;').fetchall()]

    with col1:
        st.markdown("""
    <style>
    .stSelectbox:first-of-type > div[data-baseweb="select"] > div {
              padding: 5px;
                width : 100%;
    }
                .stSelectbox>label {
                    width:400%;
                color: #A7ABC6;
font-family: Poppins;
font-size: 12px;
font-style: normal;
font-weight: 600;
line-height: normal;
    }
    </style>
""", unsafe_allow_html=True)

        selected_user = st.selectbox('User', users)

        granted_roles_data = fetch_roles_for_user(selected_user)

        granted_roles = [row['Role Name'] for row in granted_roles_data]

    # Fetch all roles and filter out the roles already granted
    all_roles = fetch_all_roles()

    roles_to_display = list(set(all_roles) - set(granted_roles))

    with col2:
            st.markdown("""
    <style>
        .stMultiSelect [data-baseweb=select] span{
            padding: 5px;
            width : 100%;
            max-width: 250px;
            font-size: 0.6rem;
        }
                        .stMultiSelect>label {
                color: #CBCACA;
              padding: 5px;
                width : 100%;
    }
    </style>
    """, unsafe_allow_html=True)
            roles_to_grant = st.multiselect('Roles', roles_to_display)



    with col3:
        st.markdown(get_css_for_button(), unsafe_allow_html=True)
        if st.button('Assign'):
            try:

                if not roles_to_grant:
                    st.warning('**Please select roles to grant.**')

                else:

                    result_message = grant_roles_and_log_using_sp(selected_user, roles_to_grant)


            except:
                pass

    df = pd.DataFrame.from_dict(granted_roles_data)

    if not granted_roles_data:
        st.warning(f'No roles assigned to {selected_user} yet.')

    else:
        st.markdown("""
                <style>.stDataeditor>{
                  background-color: 8017f5;
                }
                    </style>""",unsafe_allow_html=True)
        edited_df = st.data_editor(
     df,
    hide_index=True,
)
    if result_message:
        st.write(result_message)
    con.close()




def get_css_for_button():
    return """ <style>

.stButton>button {
    background: linear-gradient(to right, #a02a41 0%,    #1D4077 100%);
    font-family: Poppins;
    padding: 10px 30px;
    color: white;
    border-radius: 50px;
    margin : 38px;

}
</style>"""
def monitor():
    col1, col2 = st.columns([1,20])

    with col1:
        im = Image.open("Monitor-Title.png")
        st.image(im, width=24)

    with col2:
        st.markdown("<p style='color: #1D4077;font-family: Poppins;font-size: 18px;font-style: normal;font-weight: 600;line-height: normal;'>Credits Usage</p>", unsafe_allow_html=True)

    st.markdown("""
    <style>
        /* Styles for the tabs */
        .stTabs [data-baseweb="tab-list"] {
            gap: 20px;
        }

        .stTabs [data-baseweb="tab"] {
            color: grey;
            font-family: Poppins;
            font-size: 14px;
            font-style: normal;
            font-weight: 400;
            line-height: normal;
        }

        .stTabs [aria-selected="true"] {
            color: red;
            background-color: #FFFFFF;
        }
    </style>""", unsafe_allow_html=True)

    # Tabs for "Account Usage" and "Detail Metrics"
    tab1, tab2 = st.tabs(["**Account Usage**", "**Detail Metrics**"])

    with tab1:
        monitor2(1)  # Passing 1 as the tab_id

    with tab2:
        monitor3(2)  # Passing 2 as the tab_id

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
            WHERE LEFT(object_name, 4) IN ({environments_str}) AND domain = 'WAREHOUSE'
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
            WHERE LEFT(object_name, 4) IN ({environments_str}) AND domain = 'WAREHOUSE' AND tag_name = 'SUBJECT_AREA'
            AND object_name IN (
                SELECT DISTINCT tr.object_name
                FROM "SNOWFLAKE"."ACCOUNT_USAGE".TAG_REFERENCES tr
                WHERE tr.tag_name = 'COST_CENTER' AND tr.tag_value IN ({projects_str})
            );
        """
    elif environments:
        return f"""
            SELECT DISTINCT trim(tag_value)
            FROM "SNOWFLAKE"."ACCOUNT_USAGE".TAG_REFERENCES
            WHERE LEFT(object_name, 4) IN ({environments_str}) AND domain = 'WAREHOUSE' AND tag_name = 'SUBJECT_AREA';
        """
    elif projects:
        return f"""
            SELECT DISTINCT trim(tag_value)
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
            WHERE LEFT(warehouse_name, 4) = '{environment}'
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
            where_clauses.append(f"LEFT(warehouse_name, 4) IN ({environments_str})")
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
        WHERE LEFT(wmh.warehouse_name, 4) = '{environment}'
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
        AND LEFT(warehouse_name, 4) = '{environment}'
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
        fig_hourly = px.area(combined_hourly_credits, x='Hour', y='Credits', title='Credits Used Per Hour')
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

def monitor3(tab_id):

    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)

    custom_css = get_custom_css()

    st.markdown(custom_css, unsafe_allow_html=True)



    # Create columns for filters

    col_date, col_env, col_bu, col_proj = st.columns([4.5, 4.5, 4.5, 4.5])



    # Date Range Filter

    with col_date:

        date_option = st.selectbox('Select Date Range', ['1 day', '7 days', '28 days', '1 year'])

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

    with col_env:

        environments = ['All', 'DEV_', 'PROD', 'STAG', 'TEST']

        selected_environments = st.multiselect('ENVIRONMENT :', environments, default=['All'])



    # Business Unit Filter

    with col_bu:

        if selected_environments:

            projects = ['All'] + [result[0].strip() for result in execute_query(conn, construct_project_query(selected_environments)) if result[0] is not None and result[0].strip() != '']

            selected_projects = st.multiselect('BUSINESS UNIT :', projects, default=['All'])

        else:

            selected_projects = ['All']



    # Subject Area Filter

    with col_proj:

        subject_areas = ['All'] + [result[0].strip() for result in execute_query(conn, construct_subject_query(selected_environments, selected_projects))]

        selected_subject_areas = st.multiselect('PROJECT :', subject_areas, default=['All'])



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
        st.write("**Top 5 Warehouses by Credits(percentage)**")
        gui.space()  # Adding custom space using your utility function
        fig = px.pie(top_5_warehouses, names='Warehouse', values='Credits')
        st.plotly_chart(fig)
    # Hourly Credits
    if 'All' in selected_environments:
        display_hourly_credits_chart(conn, environments[1:], start_date)  # Exclude 'All' from selected environments
    else:
        display_hourly_credits_chart(conn, selected_environments, start_date)
        # Total Credits
    if 'All' in selected_environments:
        selected_env_str = ', '.join([f"'{env[:4]}'" for env in environments[1:]])
    else:
        selected_env_str = ', '.join([f"'{env[:4]}'" for env in selected_environments])
#queries
    if 'All' in selected_environments:
            filter_condition = "1=1"  # Default condition when 'All' is selected
    else:
            selected_env_str = ', '.join([f"'{env[:4]}'" for env in selected_environments])
            filter_condition = f"LEFT(warehouse_name, 4) IN ({selected_env_str})"
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
      # Define a function to fetch and display top 5 warehouses performance by query type
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
            selected_env_str = ', '.join([f"'{env[:4]}'" for env in selected_environments])
            query_performance_by_query_type = f"""
                SELECT
                    warehouse_name,
                    query_type,
                    AVG(execution_time) AS avg_execution_time_seconds
                FROM
                    snowflake.account_usage.query_history
                WHERE
                    LEFT(warehouse_name, 4) IN ({selected_env_str})
                GROUP BY
                    warehouse_name, query_type
                ORDER BY
                    warehouse_name, avg_execution_time_seconds DESC
                LIMIT 5;
            """
        performance_by_query_type_data = execute_query(conn, query_performance_by_query_type)
        if not performance_by_query_type_data:
            st.warning("No data available for top 5 warehouses performance by query type.")
        else:
            df_performance_by_query_type = pd.DataFrame(performance_by_query_type_data, columns=[
                'Warehouse Name', 'Query Type', 'Average Execution Time (seconds)'
            ])
            # Create a bar chart to display top 5 warehouse performance by query type
            df_performance_by_query_type = df_performance_by_query_type.sort_values(by='Average Execution Time (seconds)', ascending=False)
            fig_performance_by_query_type = px.bar(df_performance_by_query_type, x='Warehouse Name', y='Average Execution Time (seconds)', color='Query Type',
                                                title='Top 5 Warehouses Performance by Query Type')
            st.plotly_chart(fig_performance_by_query_type)
    # Define a function to fetch and display top 5 credits used by Cloud Services and Compute by Warehouse
    def display_top_5_credits_by_warehouse(conn, selected_environments):
        if 'All' in selected_environments:
            query_credits_by_warehouse = """
                SELECT
                    warehouse_name,
                    SUM(credits_used_cloud_services) AS credits_used_cloud_services,
                    SUM(credits_used_compute) AS credits_used_compute
                FROM
                    snowflake.account_usage.warehouse_metering_history
                GROUP BY
                    1
                ORDER BY
                    credits_used_cloud_services DESC  -- Order by credits used by Cloud Services (or 'credits_used_compute' for Compute)
                LIMIT
                    5;  -- Limit to top 5
            """
        else:
            selected_env_str = ', '.join([f"'{env}'" for env in selected_environments])
            query_credits_by_warehouse = f"""
                SELECT
                    warehouse_name,
                    SUM(credits_used_cloud_services) AS credits_used_cloud_services,
                    SUM(credits_used_compute) AS credits_used_compute
                FROM
                    snowflake.account_usage.warehouse_metering_history
                WHERE
                    LEFT(warehouse_name, 4) IN ({selected_env_str})
                GROUP BY
                    1
                ORDER BY
                    credits_used_cloud_services DESC  -- Order by credits used by Cloud Services (or 'credits_used_compute' for Compute)
                LIMIT
                    5;  -- Limit to top 5
            """
        credits_by_warehouse_data = execute_query(conn, query_credits_by_warehouse)
        if not credits_by_warehouse_data:
            st.warning("No data available for credits used by warehouse.")
        else:
            df_credits_by_warehouse = pd.DataFrame(credits_by_warehouse_data, columns=[
                'Warehouse Name', 'Credits Used by Cloud Services', 'Credits Used by Compute'
            ])
            # Create a bar chart to display credits used by Cloud Services and Compute by Warehouse
            fig_credits_by_warehouse = px.histogram(df_credits_by_warehouse, x='Warehouse Name', y=[
                'Credits Used by Cloud Services', 'Credits Used by Compute'
            ], title='Top 5 Warehouses by Credits Used for Cloud Services and Compute')
            # Rotate the y-axis labels for better readability
            fig_credits_by_warehouse.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig_credits_by_warehouse)
    # Call the function to display top 5 credits used by Cloud Services and Compute by Warehouse
    display_top_5_credits_by_warehouse(conn, selected_environments)
    conn.close()
def monitor2(tab_id):
    if tab_id == 1:  # Assuming 1 corresponds to "Account Usage"
        date_from, date_to = gui.date_selector()

        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        #conn.cursor().execute(query)
        gui.space(1)
        st.subheader("WAREHOUSE METERING - Account Usage")
    elif tab_id == 2:  # Assuming 2 corresponds to some other tab
        # Render content specific to the other tab
        st.subheader("WAREHOUSE METERING - Other Tab Name")
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
        "Histogram of **queries duration** (in secs)"
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
    MOST_NUM_QUERIES_SQL = """
    SELECT * FROM (
        SELECT current_account() as account,
            current_region() as region,
            user_name,
            warehouse_name,
            ROUND(SUM(execution_time)/(1000*60*60),1) exec_hrs,
            COUNT(1) AS num_queries
        FROM snowflake.account_usage.query_history
        WHERE start_time BETWEEN %(date_from)s AND %(date_to)s
        GROUP BY 1,2,3,4
    ) QRY
    ORDER BY num_queries DESC
    LIMIT 10;
    """
    # Execute the new SQL Query and get the DataFrame
    with snowflake.connector.connect(**SNOWFLAKE_CONFIG) as conn:
        cur = conn.cursor(snowflake.connector.DictCursor)
        cur.execute(
            MOST_NUM_QUERIES_SQL,
            {"date_from": date_from, "date_to": date_to}
        )
        most_num_queries_df = pd.DataFrame(cur.fetchall())
        most_num_queries_df.columns = [col[0] for col in cur.description]
    # If DataFrame is not empty, display the Pie Chart
    if not most_num_queries_df.empty:
        # Sort the DataFrame by 'NUM_QUERIES' in descending order
        most_num_queries_df = most_num_queries_df.sort_values(by='NUM_QUERIES', ascending=False)
        st.markdown("  \n")  # Adds a newline as a space
        st.markdown("**Distribution of Queries Executed by User**")
        # Create a pie chart using Plotly Express
        fig = px.pie(most_num_queries_df,
                    names='USER_NAME',
                    values='NUM_QUERIES')
        # Adjusting the layout for better readability and size
        fig.update_traces(textinfo='label+percent')  # 'label+percent' shows the label and percentage on the pie chart.
        fig.update_layout(margin={"r":0,"t":40,"l":0,"b":0}, width=1000, height=500)  # Adjusting margins and size
        # Display the graph
        st.plotly_chart(fig)

    else:
        st.write("No data available for the given date range.")
    MOST_EXEC_HRS_SQL = """
    SELECT * FROM (
        SELECT current_account() as account,
            current_region() as region,
            user_name,
            warehouse_name,
            ROUND(SUM(execution_time)/(1000*60*60),1) exec_hrs,
            COUNT(1) AS num_queries
        FROM snowflake.account_usage.query_history
        WHERE start_time BETWEEN %(date_from)s AND %(date_to)s
        GROUP BY 1,2,3,4
    ) QRY
    ORDER BY exec_hrs DESC
    LIMIT 10;
    """
    # Correctly execute the new SQL Query and get the DataFrame
    with snowflake.connector.connect(**SNOWFLAKE_CONFIG) as conn:
        cur = conn.cursor(snowflake.connector.DictCursor)
        cur.execute(MOST_EXEC_HRS_SQL, {"date_from": date_from, "date_to": date_to})
        most_exec_hrs_df = pd.DataFrame(cur.fetchall())
        most_exec_hrs_df.columns = [col[0] for col in cur.description]
# If DataFrame is not empty, display the Bar Chart
    if not most_exec_hrs_df.empty:
        # Create a Plotly Bar Chart without displaying execution hours on the bars
        fig = px.bar(most_exec_hrs_df,
                    x='USER_NAME',
                    y='EXEC_HRS',
                    labels={'USER_NAME': 'User Name', 'EXEC_HRS': 'Execution Hours'},
                    title='Users with Most Query Execution Hours')
        # Update layout for a cleaner appearance
        fig.update_layout(
            uniformtext_minsize=8,
            uniformtext_mode='hide',
            xaxis_title='Users',
            yaxis_title='Execution Hours (hrs)',
            bargap=0.2,  # gap between bars
            bargroupgap=0.1  # gap between groups
        )
        # Display the Plotly Chart
        st.plotly_chart(fig)
    else:
        st.write("No data available for the given date range.")
    LOW_ACCESS_HIGH_VOLUME_SQL = """
    SELECT * FROM
    (
        select distinct current_account() as account, current_region() as region, ROW_COUNT,  TAB.TABLE_NAME, TABLE_TYPE, DATABASE_NAME, SCHEMA_NAME, USERS, QUERIES from
        (SELECT
        split_part(f1.value:"objectName"::string, '.', 1) AS database_name
        ,split_part(f1.value:"objectName"::string, '.', 2) AS schema_name
        ,split_part(f1.value:"objectName"::string, '.', 3) AS table_name
        ,COUNT(DISTINCT USER_NAME) AS users
        ,COUNT(DISTINCT QUERY_ID) AS queries
        FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
        , lateral flatten(base_objects_accessed) f1  GROUP BY 1,2,3) QH,
        (select ROW_COUNT,TABLE_NAME, TABLE_SCHEMA, TABLE_TYPE from "SNOWFLAKE"."ACCOUNT_USAGE"."TABLES"
        where DELETED is null) TAB where TAB.TABLE_NAME = QH.TABLE_NAME
        and (QH.USERS < 2 and QH.QUERIES < 5)
    )
    order by ROW_COUNT desc,9,8
    limit 10;
    """
 
def about():
    # Create an expander for the about section
    des1=option_menu(
        menu_title =None,
        options=["SNOWGOV"],
        icons = ["snow2"],

    )
    if des1 == "SNOWGOV":
            st.markdown("""
            <div style="font-size:14px;"font-family": "Poppins"">
                <p style="font-size:14px;">This project is to demonstrate the power of Snowflake Native Apps. The objective of this project is to develop an App that provides GUI-based governance features for managing the Snowflake environment. Some of the features include:</p>
                <ul style="font-size:14px;">
                    <li style="font-size:14px;">User interface through which the IT team can configure Organization and Account Parameters</li>
                    <li style="font-size:14px;">User Interface through which IT teams can create Projects (a logical entity). For each Project, they can create multiple Environments (Dev, Stage, Production). Internally for each environment, the app creates Databases or schemas depending on configuration. For each project and environment, provide a GUI to create warehouses</li>
                    <li style="font-size:14px;">Onboard users to projects and assign respective roles on each environment (i.e. Database or Schemas)</li>
                    <li style="font-size:14px;">Provide Cost-monitoring dashboards drilled down by Accounts, Projects, Environments, Users, etc.</li>
                </ul>
            </div>
            """, unsafe_allow_html=True)
    faq=option_menu(
        menu_title =None,
        options=["FAQ's"],
        icons =["bookmarks-fill"],

    )
    if faq == "FAQ's":
        with st.expander("**What is SnowGov, and why do I need it?**",expanded=False):
            st.markdown("""
                        <p style="font-size:14px;">SnowGov is a powerful Snowflake Native App designed to simplify the management and governance of your Snowflake environment. You need SnowGov to efficiently configure accounts, create projects and environments, manage users, and monitor costs in your Snowflake setup.</p>""", unsafe_allow_html=True)
        with st.expander("**How can I configure my organization and account parameters with SnowGov?**",expanded=False):
            st.markdown("""
                        <p style="font-size:14px;">SnowGov provides an intuitive user interface for your IT team to easily configure organization and account parameters, ensuring you have full control over your Snowflake resources.</p>""", unsafe_allow_html=True)
        with st.expander("**Can I create logical entities like projects and environments with SnowGov?**",expanded=False):
            st.markdown('''<p style="font-size:14px;">Yes, SnowGov allows you to create projects and multiple environments within them, such as Development, Staging, and Production. It also dynamically generates databases or schemas based on your configurations.</p>''', unsafe_allow_html=True)
        with st.expander('**How do I manage warehouses for my projects and environments using SnowGov?**',expanded=False):
            st.markdown('''<p style="font-size:14px;">SnowGov offers a user-friendly GUI to create and manage warehouses for each project and environment, simplifying Snowflake resource allocation.</p>''', unsafe_allow_html=True)
        with st.expander("**How can I onboard users to projects and assign roles within each environment?**",expanded=False):
            st.markdown('''<p style="font-size:14px;">SnowGov streamlines user onboarding and role assignment, ensuring that users have the right access permissions within databases or schemas.</p>''', unsafe_allow_html=True)
        with st.expander('**Can I monitor Snowflake costs with SnowGov?**',expanded=False):
             st.markdown('''<p style="font-size:14px;">Yes, SnowGov provides cost-monitoring dashboards that allow you to track costs by accounts, projects, environments, and users, helping you optimize your Snowflake spending.</p>''', unsafe_allow_html=True)
def Menu_navigator():
    with st.sidebar:
        choice = option_menu(
           menu_title="",
            options=["User","Database" ,"Role", "Monitor","About"],
            icons=["people-fill","database-fill", "person-lines-fill", "tv-fill","info-circle-fill"],
            menu_icon="menu-button-wide-fill",

        )
    pages = {
        "User Creation": user_creation_page,
        "Database Management": database_management,
        "Role Management" : role_manage,
        "Monitor" : monitor,
        "About"   : about
    }
    styles={
        "container": {"padding": "0!important", "background-color": "#fafafa"},
        "nav-link": {"font-family":"","font-size": "18px", "text-align": "left", "margin":"0px", "--hover-color": "#eee"},
        "nav-link-selected": {"background-color": "#0096FF"},
      }
    if choice == 'Database':
        current_page = "Database Management"
    elif choice == 'User':
        current_page = "User Creation"
    elif choice == 'Role':
        current_page = "Role Management"
    elif choice == 'Monitor':
        current_page = "Monitor"
    elif choice == 'About':
        current_page = "About"
    # Add more elif conditions if you have more choices/pages
    st.session_state.current_page = current_page
    pages[current_page]()
    st.sidebar.markdown("</div>", unsafe_allow_html=True)
# Main function with CSS applied at the beginning

def customize_footer():
    st.markdown("""
        <style>
            /* Hide default footer */
            .reportview-container .main footer {visibility: hidden;}

            /* Add a new footer */
            .footer:after {
                content:'Powered by Anblicks';
                visibility: visible;
                display: block;
                position: fixed;
                /* Adjust to place it at the bottom right */
                right: 10rem;
                bottom: 10px;
                font-size: 1rem;
                color: gray;
                z-index: 1000;
            }
        </style>
        <div class="footer"></div>
    """, unsafe_allow_html=True)

def main():
    st.markdown("""
        <style>
            .stApp {
                margin-left: 0!important;
                padding-left: 0!important;
            }
        </style>
    """, unsafe_allow_html=True)
    
    custom_css = get_custom_css()
    st.markdown(custom_css, unsafe_allow_html=True)
    
    st.markdown("<style>body {background-color: #3498DB;}</style>", unsafe_allow_html=True)
    st.markdown("<style>.stButton>button {background-color: #2980B9;font-family:Poppins; color: white;}</style>", unsafe_allow_html=True)
    
    if "conn" not in st.session_state:
        st.session_state.conn = None
    if "connections" not in st.session_state:
        st.session_state.connections = {}
    
    Menu_navigator()

if __name__ == "__main__":
    main()

    customize_footer()

