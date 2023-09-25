import snowflake.connector

import streamlit as st

import pandas as pd

import plotly.express as px

from datetime import datetime, timedelta

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


    environments = ['All', 'DEV', 'PROD', 'STAGE', 'TEST']

    selected_environments = st.sidebar.multiselect('ENVIRONMENT :', environments, default=['All'])

    if not selected_environments:
        st.warning("Please select at least one option for the Environment filter.")
        return


    if selected_environments:
        projects = ['All'] + [result[0].strip() for result in execute_query(conn, construct_project_query(selected_environments))]

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



    # Define a variable to conditionally select all or top three warehouses
    top_warehouses_condition = ""

    # Construct the SQL query based on the selected environments
    if 'All' not in selected_environments:
        top_warehouses_condition = f"""
            AND warehouse_name IN (
                SELECT warehouse_name
                FROM (
                    SELECT warehouse_name, SUM(credits_used) AS total_credits_used
                    FROM "SNOWFLAKE"."ACCOUNT_USAGE".WAREHOUSE_METERING_HISTORY
                    WHERE LEFT(warehouse_name, 3) IN ({selected_env_str})
                        AND start_time BETWEEN '{start_date.strftime('%Y-%m-%d')}' AND '{end_date.strftime('%Y-%m-%d')}'
                    GROUP BY warehouse_name
                    ORDER BY total_credits_used DESC
                    LIMIT 3
                )
            )
        """

    query_total_credits = f"""
        SELECT start_time::date AS day, warehouse_name, SUM(credits_used) AS total_credits_used
        FROM "SNOWFLAKE"."ACCOUNT_USAGE".WAREHOUSE_METERING_HISTORY
        WHERE start_time BETWEEN '{start_date.strftime('%Y-%m-%d')}' AND '{end_date.strftime('%Y-%m-%d')}'
        {top_warehouses_condition}
        GROUP BY day, warehouse_name
        ORDER BY day, warehouse_name;
    """

    total_credits_data = execute_query(conn, query_total_credits)

    if not total_credits_data:
        st.warning("No data available for the selected filters.")
    else:
        df_total_credits = pd.DataFrame(total_credits_data, columns=['Day', 'Warehouse', 'Total Credits Used'])

        # Group the DataFrame by 'Warehouse' and calculate the total credits used per warehouse

        df_total_credits['Total Credits Used'] = df_total_credits.groupby('Warehouse')['Total Credits Used'].transform('sum')

        # Calculate the usage percentage
        total_credits_sum = float(df_total_credits['Total Credits Used'].sum())
        df_total_credits['Usage Percentage'] = (df_total_credits['Total Credits Used'].astype(float) / total_credits_sum) * 100

        # Round the percentage to 2 decimal places
        df_total_credits['Usage Percentage'] = df_total_credits['Usage Percentage'].round(2)

        # Create a pie chart to display Usage Percentage with Warehouse names outside the chart

    fig_percentage = px.pie(df_total_credits, names='Warehouse', values='Usage Percentage')

    # Add annotations for warehouse names outside the pie chart
    annotations = []

    # Define initial positions for labels
    x_positions = [7.2, 6.7, 7.2]  # Adjust these values as needed
    y_positions = [5.3, 5.1, 5.9]  # Adjust these values as needed

    for warehouse, percent, x, y in zip(df_total_credits['Warehouse'], df_total_credits['Usage Percentage'], x_positions, y_positions):
        annotations.append(dict(
            text=warehouse,
            x=x,
            y=y,
            font=dict(size=10),  # Adjust the font size as needed
            showarrow=False
        ))

    fig_percentage.update_traces(textinfo='percent+label', pull=[0.0005, 0.0005, 0.0005])  # Adjust pull to move slices away from the center
    fig_percentage.update_layout(yaxis_tickformat=".2f", annotations=annotations)

    # Display the pie chart for Usage Percentage
    st.markdown("**Credit Usage Overtime**")  # Add the line here

    st.plotly_chart(fig_percentage)


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

    # Define a function to fetch and display top 5 warehouses by credits used
    def display_top_5_warehouses_by_credits(conn, selected_environments):
        # Construct the SQL query based on the selected environments
        if 'All' in selected_environments:
            query_top_5_warehouses = """
                SELECT
                    warehouse_name,
                    SUM(credits_used) AS total_credits_used
                FROM
                    snowflake.account_usage.warehouse_metering_history
                GROUP BY
                    1
                ORDER BY
                    2 DESC
                LIMIT
                    5;
            """
        else:
            selected_env_str = ', '.join([f"'{env[:3]}'" for env in selected_environments])
            query_top_5_warehouses = f"""
                SELECT
                    warehouse_name,
                    SUM(credits_used) AS total_credits_used
                FROM
                    snowflake.account_usage.warehouse_metering_history
                WHERE
                    LEFT(warehouse_name, 3) IN ({selected_env_str})
                GROUP BY
                    1
                ORDER BY
                    2 DESC
                LIMIT
                    5;
            """

        top_5_warehouses_data = execute_query(conn, query_top_5_warehouses)

        if not top_5_warehouses_data:
            st.warning("No data available for the selected filters.")
        else:
            df_top_5_warehouses = pd.DataFrame(top_5_warehouses_data, columns=[
                'Warehouse Name', 'Total Credits Used'
            ])

            # Create a bar chart to display top 5 warehouses by credits used
            fig_top_5_warehouses = px.bar(df_top_5_warehouses, x='Warehouse Name', y='Total Credits Used', title='Credits used by Cloud Services + compute by Warehouse')
            st.plotly_chart(fig_top_5_warehouses)

    # Call the function to display top 5 warehouses by credits used
    display_top_5_warehouses_by_credits(conn, selected_environments)

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

            # Create a bar chart to display top 5 warehouse performance by query type
            fig_performance_by_query_type = px.bar(df_performance_by_query_type, x='Warehouse Name', y='Average Execution Time (seconds)', color='Query Type',
                                                title='Top 5 Warehouse Performance by Query Type')
            st.plotly_chart(fig_performance_by_query_type)


    # ... (your existing code)

    display_top_5_warehouse_performance_by_query_type(conn, selected_environments)








    conn.close()



# Replace SNOWFLAKE_CONFIG with your actual configuration

SNOWFLAKE_CONFIG = {

    "account": "xh84085.ap-southeast-1",

    "user": "sravani12",

    "password": "Sravani@12",

    "role": "accountadmin",

    "warehouse": "COMPUTE_WH",

    "database": "UTIL_DB",

    "schema": "ADMIN_TOOLS"

}

monitor3()
