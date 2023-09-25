import streamlit as st
import snowflake.connector
import base64

# Setup layout and styling
st.set_page_config(
    page_title="Admin Login",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Simple in-memory cache
_memory_cache = {}

def get_img_as_base64(file):
    if file in _memory_cache:
        return _memory_cache[file]

    with open(file, "rb") as f:
        data = f.read()
    encoded_data = base64.b64encode(data).decode()
    _memory_cache[file] = encoded_data

    return encoded_data

# Getting the base64 encoding of the image
img = get_img_as_base64("C:\\Users\\sravani.sammu\\Downloads\\i.jpg")

# Setting the background image using inline CSS
# Setting the background image using inline CSS
page_bg_img = f"""
<style>
.main {{
    background-color: white;
    background-image: linear-gradient(rgba(255,255,255,0.5), rgba(255,255,255,0.5)), url("data:image/png;base64,{img}");
    background-size: 50% 100%;
    background-repeat: no-repeat;
    width: 100vw;
    margin-top: 0 !important;  # remove the space above
}}
</style>
"""

# Rest of your code remains the same


# Applying the background image to the Streamlit app
st.markdown(page_bg_img, unsafe_allow_html=True)

# Initialize session state for user login
if 'loggedin' not in st.session_state:
    st.session_state.loggedin = False

def connect_to_snowflake(conn_params):
    return snowflake.connector.connect(**conn_params)

# Grouping Login and Connector Page
with st.container():
    col1, col2 = st.columns((1, 1))
    with col1:
        st.write("")  # Empty space
    with col2:
        # Container for login
        with st.container():
            # Login Page
            if not st.session_state.loggedin:
                st.subheader("Welcome back!")
                username = st.text_input("Username")
                password = st.text_input("Password", type="password")
                if st.button('Login'):
                    if username == 'admin' and password == 'admin':
                        st.session_state.loggedin = True
                    else:
                        st.error('Invalid username or password')

        # Connector Page
        if st.session_state.loggedin:
            with st.expander("Snowflake Connector"):
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
                    except snowflake.connector.errors.DatabaseError as e:
                        st.error(f"🚫 Connection failed. Error: {e}")
