import mysql.connector
import streamlit as st
import pandas as pd
from datetime import datetime, date
from st_aggrid import AgGrid, GridOptionsBuilder
import time
import json

st.set_page_config(layout="wide", page_title="Steam Games Management", page_icon="🎮")

COLUMN_RENAME_MAP = {
    "game_id": "ID",
    "name": "Name",
    "release_date": "Release Date",
    "required_age": "Required Age",
    "price": "Price ($)",
    "windows": "Windows",
    "mac": "Mac",
    "linux": "Linux",
    "languages": "Languages",
    "developers": "Developers",
    "publishers": "Publishers",
    "genres": "Genres",
}

LOG_FILE = "recovery_log.txt"  # Log file path (still in the current working directory)
REPLICATION_LAG = 15  # Simulate replication lag (seconds)

def create_connection(connection_key):
    # Retrieve the connection details from secrets.toml
    host = st.secrets[connection_key]["host"]
    port = st.secrets[connection_key]["port"]
    user = st.secrets[connection_key]["username"]
    password = st.secrets[connection_key]["password"]
    database = st.secrets[connection_key]["database"]

    conn = mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database
    )
    return conn

# Create connections using the keys defined in secrets.toml
conn1 = create_connection("node_1")
conn2 = create_connection("node_2")
conn3 = create_connection("node_3")
conn1_cursor = conn1.cursor()
conn2_cursor = conn2.cursor()
conn3_cursor = conn3.cursor()

# Function to check if a connection is working
def is_connection_active(conn):
    try:
        conn.ping()  # Try to ping the connection to check if it's alive
        return True
    except Exception:
        return False
    
# Function to fetch data from a connection
def fetch_data(conn, query):
    cursor = conn.cursor(dictionary=True)
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    return pd.DataFrame(result)

# Node Status
node_status = {
    "Node 1": True,
    "Node 2": True,
    "Node 3": True,
}

def fetch_data_with_fallback(query):
    # Try to fetch from Node 1
    if is_connection_active(conn1):
        df = fetch_data(conn1, query)
    else:
        # If Node 1 is down, use Node 2 or Node 3 based on the year partition
        print("Node 1 is down, using fallback...")
        
        # Attempt to get the release year to decide the partition node
        game_id = 1  # Example: you can specify a game_id to check its release year
        cursor = conn2_cursor if is_connection_active(conn2) else conn3_cursor
        conn = conn2 if is_connection_active(conn2) else conn3
        
        cursor.execute("SELECT release_date FROM games WHERE game_id = %s", (game_id,))
        result = cursor.fetchone()
        
        if result:
            year = result[0].year
            if year <= 2010:
                print("Using Node 2")
                df = fetch_data(conn2, query)
            else:
                print("Using Node 3")
                df = fetch_data(conn3, query)
        else:
            print("Error: Could not find release date for the game.")
            df = pd.DataFrame()  # Return an empty DataFrame if no data found

    return df

# Query to fetch data from the games table
query = "SELECT * FROM games"
df = fetch_data_with_fallback(query)

if 'df' not in st.session_state:
    st.session_state.df = df

def date_helper(date):
    return date.strftime("%Y")

def display_table(df):
    df["release_date"] = pd.to_datetime(df["release_date"]).dt.strftime("%Y-%m-%d")
    
    df["Windows"] = df["windows"].map({1: "✔️", 0: "❌"})
    df["Mac"] = df["mac"].map({1: "✔️", 0: "❌"})
    df["Linux"] = df["linux"].map({1: "✔️", 0: "❌"})
    df = df.drop(columns=["windows", "mac", "linux"]) 

    display_df = df.rename(columns=COLUMN_RENAME_MAP)

    gb = GridOptionsBuilder.from_dataframe(display_df)
    gb.configure_pagination(paginationAutoPageSize=True)  

    gb.configure_column("ID", width=130)
    gb.configure_column("Name", width=300)
    gb.configure_column("Release Date", width=160)
    gb.configure_column("Required Age", width=175)
    gb.configure_column("Price ($)", width=130)
    gb.configure_column("Windows", headerName="Windows", width=150)
    gb.configure_column("Mac", headerName="Mac", width=150)
    gb.configure_column("Linux", headerName="Linux", width=150)

    gb.configure_default_column(minWidth=0, maxWidth=300)  
    grid_options = gb.build()

    AgGrid(
        display_df,
        gridOptions=grid_options,
        height=500,
        fit_columns_on_grid_load=True,
        enable_enterprise_modules=False,
        theme="streamlit",  
    )

# Function to handle datetime and date conversion
def datetime_converter(obj):
    if isinstance(obj, datetime):
        return obj.strftime('%Y-%m-%d %H:%M:%S')
    elif isinstance(obj, date):
        return obj.strftime('%Y-%m-%d')
    return obj

# Utility: Simulate replication lag
def simulate_replication_lag():
    time.sleep(REPLICATION_LAG)

# Automatic Recovery for a Node
def recover_node(node, conn, cursor):
    try:
        with open(LOG_FILE, "r") as log:
            for line in log:
                entry = json.loads(line.strip())
                if entry["node"] != node:
                    params = entry["params"]
                    cursor.execute(entry["query"], params)
                    conn.commit()
        st.success(f"Automatic recovery completed for {node}.")
    except Exception as e:
        st.error(f"Error during recovery for {node}: {e}")

# Function to log transactions (simulate database insertion)
def log_transaction(action, node, query, params):
    try:
        params = [
            datetime_converter(param) if isinstance(param, (datetime, date)) else param
            for param in params
        ]
        log_entry = {
            "action": action,
            "node": node,
            "query": query,
            "params": params,
        }
        with open(LOG_FILE, "a") as log_file:
            log_file.write(json.dumps(log_entry) + "\n")
    except Exception as e:
        st.error(f"Error logging transaction: {e}")
        raise

MAX_RETRIES = 1
RETRY_DELAY = 2  # Delay in seconds before retrying

def replicate_from_temp_logs_to_node_1():
    try:
        with open(LOG_FILE, "r") as log:
            lines = log.readlines()

        if not lines:
            return  # No logs to process

        updated_logs = []
        for line in lines:
            try:
                entry = json.loads(line.strip())
                action = entry["action"]
                query = entry["query"]
                params = entry["params"]

                if action.endswith("_TEMP") and entry["node"] != "Node 1":
                    # Simulate failure when attempting to replicate to Node 1 from Node 2 or Node 3
                    if st.session_state.get("simulate_failure_node_1", False):
                        log_transaction("REPLICATE_FAILURE", "Node 1", query, params)
                        raise Exception("Simulated failure while replicating to Node 1 from Node 2 or Node 3.")
                    attempt = 0
                    while attempt < MAX_RETRIES:
                        try:
                            if action.startswith("INSERT"):
                                conn1_cursor.execute(query, params)
                            elif action.startswith("UPDATE"):
                                conn1_cursor.execute(query, params)
                            elif action.startswith("DELETE"):
                                conn1_cursor.execute(query, params)

                            conn1.commit()
                            log_transaction(action.replace("_TEMP", "_REPLICATED"), "Node 1", query, params)
                            st.success(f"{action.replace('_TEMP', '')} operation replicated to Node 1 successfully.")
                            break
                        except Exception as e:
                            attempt += 1
                            if attempt < MAX_RETRIES:
                                st.warning(f"Retrying {action} to Node 1 in {RETRY_DELAY} seconds... (Attempt {attempt}/{MAX_RETRIES})")
                                time.sleep(RETRY_DELAY)
                            else:
                                st.error(f"Failed to replicate {action} to Node 1 after {MAX_RETRIES} attempts: {e}")
                                updated_logs.append(line)
                                break
                else:
                    updated_logs.append(line)  # Retain logs not meant for replication to Node 1

            except json.JSONDecodeError:
                st.warning("Skipping invalid log entry.")
                continue

        with open(LOG_FILE, "w") as log:
            log.writelines(updated_logs)

    except Exception as e:
        st.error(f"Error during replication: {e}")


backup_node = None
def replicate_from_temp_logs_to_backup_node():
    try:
        with open(LOG_FILE, "r") as log:
            lines = log.readlines()

        if not lines:
            return  # No logs to process

        updated_logs = []
        for line in lines:
            try:
                entry = json.loads(line.strip())
                action = entry["action"]
                node = entry["node"]
                query = entry["query"]
                params = entry["params"]

                if action.endswith("_TEMP"):
                    if st.session_state.get("simulate_failure_node_2or3", False):
                        if node == "Node 2":
                            log_transaction("REPLICATE_FAILURE", "Node 2", query, params)
                            raise Exception("Simulated failure while replicating to Node 2 from Node 1.")
                        else:
                            log_transaction("REPLICATE_FAILURE", "Node 3", query, params)
                            raise Exception("Simulated failure while replicating to Node 3 from Node 1.")
                    attempt = 0
                    while attempt < MAX_RETRIES:
                        try:
                            if action.startswith("INSERT"):
                                cursor = conn2_cursor if node == "Node 2" else conn3_cursor
                                cursor.execute(query, params)
                            elif action.startswith("UPDATE"):
                                cursor = conn2_cursor if node == "Node 2" else conn3_cursor
                                cursor.execute(query, params)
                            elif action.startswith("DELETE"):
                                cursor = conn2_cursor if node == "Node 2" else conn3_cursor
                                cursor.execute(query, params)

                            conn = conn2 if node == "Node 2" else conn3
                            conn.commit()
                            log_transaction(action.replace("_TEMP", "_REPLICATED"), node, query, params)
                            st.success(f"{action.replace('_TEMP', '')} operation replicated to {node} successfully.")
                            break
                        except Exception as e:
                            attempt += 1
                            if attempt < MAX_RETRIES:
                                st.warning(f"Retrying {action} to {node} in {RETRY_DELAY} seconds... (Attempt {attempt}/{MAX_RETRIES})")
                                time.sleep(RETRY_DELAY)
                            else:
                                print(f"Node status of {node}: {node_status[node]} Replicating from temp logs.")
                                st.error(f"Failed to replicate {action} to {node} after {MAX_RETRIES} attempts: {e}")
                                updated_logs.append(line)
                                break
                else:
                    updated_logs.append(line)  # Retain logs not meant for replication to the backup node

            except json.JSONDecodeError:
                st.warning("Skipping invalid log entry.")
                continue

        with open(LOG_FILE, "w") as log:
            log.writelines(updated_logs)

    except Exception as e:
        st.error(f"Error during replication: {e}")
        
def show():
    """Display all games in the database."""
    st.header("Show Games 🎮")
    if not df.empty:
        st.write("Displaying all games in the database:")
        display_table(df)
    else:
        st.warning("No games available to display.")

def insert():
    st.header("Insert Game 🎮")
    with st.form("insert_form"):
        # Form fields
        game_id = st.number_input("Enter Game ID", min_value=1, step=1)
        name = st.text_input("Enter Name")
        release_date = st.date_input("Enter Release Date")
        required_age = st.number_input("Enter Required Age", min_value=0, step=1)
        price = st.number_input("Enter Price", min_value=0.0, step=0.01)

        # Platform checkboxes
        cols = st.columns(3)
        windows = cols[0].checkbox("Windows")
        mac = cols[1].checkbox("Mac")
        linux = cols[2].checkbox("Linux")

        # Additional attributes
        languages = st.text_input("Enter Languages (comma-separated)")
        developers = st.text_input("Enter Developers (comma-separated)")
        publishers = st.text_input("Enter Publishers (comma-separated)")
        genres = st.text_input("Enter Genres (comma-separated)")

        submitted = st.form_submit_button("Submit")

    if submitted:
        query = """
            INSERT INTO games (
                game_id, name, release_date, required_age, price,
                windows, mac, linux, languages, developers, publishers, genres
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = (
            game_id, name, release_date, required_age, price,
            int(windows), int(mac), int(linux),
            languages, developers, publishers, genres
        )
        year = release_date.year

        try:
            if node_status["Node 1"]:
                # If Node 1 is up
                global backup_node 
                conn1_cursor.execute(query, params)
                conn1.commit()
                st.info(f"Data inserted into Node 1.")

                # Depending on the year, log for replication to the backup node (Node 2 or Node 3)
                if(node_status["Node 1"] and (node_status["Node 2"] == False or node_status["Node 3"]== False)):
                    backup_node = "Node 2" if year < 2010 else "Node 3"
                    log_transaction("INSERT_TEMP", backup_node, query, params)
                    st.info(f"Will replicate to {backup_node} once it comes back online")
                else:
                    backup_node = "Node 2" if year <= 2010 else "Node 3"
                    cursor = conn2_cursor if backup_node == "Node 2" else conn3_cursor
                    conn = conn2 if backup_node == "Node 2" else conn3
                    cursor.execute(query, params)
                    conn.commit()
                    log_transaction("INSERT", backup_node, query, params)
                    st.success(f"Game successfully inserted into {backup_node}.")
            else:
                # If Node 1 is down
                backup_node = "Node 2" if year <= 2010 else "Node 3"
                cursor = conn2_cursor if backup_node == "Node 2" else conn3_cursor
                conn = conn2 if backup_node == "Node 2" else conn3
                cursor.execute(query, params)
                conn.commit()
                log_transaction("INSERT_TEMP", backup_node, query, params)
                st.warning(f"Node 1 is down, inserted into {backup_node} instead.")
                log_transaction("INSERT_TEMP", "Node 1", query, params)
                st.info("Will replicate to Node 1 once it comes back online.")

                # Attempt replication to Node 1 if it comes back online
                if node_status["Node 1"]:
                    replicate_from_temp_logs_to_node_1()

        except Exception as e:
            st.error(f"Error inserting game: {e}")

        finally:
            # Update the DataFrame after insert
            st.session_state.df = fetch_data(conn1, "SELECT * FROM games")


def search():
    st.header("Search Game 🔍")
    with st.form("search_form"):
        search_term = st.text_input("Search by Game ID")
        submitted = st.form_submit_button("Search")

        if submitted:
            try:
                search_query = f"SELECT * FROM games WHERE game_id = '{search_term}' FOR UPDATE;"
                search_results = fetch_data(conn1, search_query)

                if not search_results.empty:
                    game = search_results.iloc[0]  # Get the first and only row

                    st.write(f"Displaying search results for Game ID '{search_term}':")

                    st.subheader(f"Details for Game ID: {game['game_id']} - {game['name']}")
                    st.write(f"**Release Date:** {game['release_date']}")
                    st.write(f"**Required Age:** {game['required_age']}")
                    st.write(f"**Price ($):** {game['price']}")
                    st.write(f"**Languages:** {game['languages']}")
                    st.write(f"**Developers:** {game['developers']}")
                    st.write(f"**Publishers:** {game['publishers']}")
                    st.write(f"**Genres:** {game['genres']}")
                    st.write(f"**Windows Support:** {'✔️' if game['windows'] == 1 else '❌'}")
                    st.write(f"**Mac Support:** {'✔️' if game['mac'] == 1 else '❌'}")
                    st.write(f"**Linux Support:** {'✔️' if game['linux'] == 1 else '❌'}")
                else:
                    st.warning("No game found with the provided ID.")
            except mysql.connector.Error as err:
                st.warning(f"Error: {err}")

def update():
    st.header("Update Game ✏️")

    with st.form("Search"):
        search_term = st.text_input("Search by Game ID or Name")
        submitted = st.form_submit_button("Search")
        if submitted:
            search_results = df[(df["game_id"].astype(str) == search_term) | (df["name"].str.contains(search_term, case=False, na=False))]
            display_table(search_results)

    with st.form("Update"):
        selected_id = st.number_input("Select Game ID to Update", min_value=1, step=None)
        game_to_update = df[df["game_id"] == selected_id]
        submitted = st.form_submit_button("Search")

    if not game_to_update.empty:
        game_id = game_to_update.iloc[0]["game_id"]
        with st.form("update_form"):
            name = st.text_input("Update Name", value=game_to_update.iloc[0]["name"])
            release_date = st.date_input(
                "Update Release Date", value=pd.to_datetime(game_to_update.iloc[0]["release_date"])
            )
            required_age = st.number_input(
                "Update Required Age", min_value=0)
            price = st.number_input(
                "Update Price", min_value=0.0)
            windows = st.checkbox("Windows", value=bool(game_to_update.iloc[0]["windows"]))
            mac = st.checkbox("Mac", value=bool(game_to_update.iloc[0]["mac"]))
            linux = st.checkbox("Linux", value=bool(game_to_update.iloc[0]["linux"]))
            languages = st.text_area("Update Languages", value=game_to_update.iloc[0]["languages"])
            developers = st.text_area("Update Developers", value=game_to_update.iloc[0]["developers"])
            publishers = st.text_area("Update Publishers", value=game_to_update.iloc[0]["publishers"])
            genres = st.text_area("Update Genres", value=game_to_update.iloc[0]["genres"])

            submitted = st.form_submit_button("Update")

            # Inside the Update Logic
            if submitted:
                original_year = pd.to_datetime(game_to_update.iloc[0]["release_date"]).year
                updated_year = release_date.year

                # SQL Queries
                query_update = """
                    UPDATE games 
                    SET name = %s, release_date = %s, required_age = %s, price = %s, 
                        windows = %s, mac = %s, linux = %s, languages = %s, developers = %s, 
                        publishers = %s, genres = %s 
                    WHERE game_id = %s
                """
                params_update = (
                    name, release_date, required_age, price,
                    int(windows), int(mac), int(linux), languages,
                    developers, publishers, genres, int(game_id),
                )
                query_insert = """
                    INSERT INTO games (
                        game_id, name, release_date, required_age, price,
                        windows, mac, linux, languages, developers, publishers, genres
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                params_insert = (
                    int(game_id), name, release_date, required_age, price,
                    int(windows), int(mac), int(linux), languages,
                    developers, publishers, genres
                )

                try:
                    # Node Transition Logic
                    if original_year < 2010 and updated_year >= 2010:
                        print("HAPPY PATH")
                        # Transition from Node 2 to Node 3
                        conn1_cursor.execute(query_update, params_update)
                        time.sleep(5)
                        conn1.commit()
                        log_transaction("UPDATE", "Node 1", query_update, params_update)

                        conn3_cursor.execute(query_insert, params_insert)
                        conn3.commit()
                        log_transaction("INSERT", "Node 3", query_insert, params_insert)

                        conn2_cursor.execute("DELETE FROM games WHERE game_id = %s", (int(game_id),))
                        conn2.commit()
                        log_transaction("DELETE", "Node 2", "DELETE FROM games WHERE game_id = %s", (int(game_id),))

                    elif int(original_year) >= 2010 and updated_year < 2010:
                        print("HAPPY PATH")
                        # Transition from Node 3 to Node 2
                        conn1_cursor.execute(query_update, params_update)
                        time.sleep(5)
                        conn1.commit()
                        log_transaction("UPDATE", "Node 1", query_update, params_update)

                        conn2_cursor.execute(query_insert, params_insert)
                        conn2.commit()
                        log_transaction("INSERT", "Node 2", query_insert, params_insert)

                        conn3_cursor.execute("DELETE FROM games WHERE game_id = %s", (int(game_id),))
                        conn3.commit()
                        log_transaction("DELETE", "Node 3", "DELETE FROM games WHERE game_id = %s", (int(game_id),))

                    elif (int(original_year) and updated_year) >= 2010 or (int(original_year) and updated_year) < 2010:
                        conn1_cursor.execute(query_update, params_update)
                        time.sleep(5)
                        conn1.commit()
                        log_transaction("UPDATE", "Node 1", query_update, params_update)
                        if updated_year < 2010:
                            conn2_cursor.execute(query_update, params_update)
                            conn2.commit()
                            log_transaction("UPDATE", "Node 2", query_update, params_insert)
                        else:
                            conn3_cursor.execute(query_update, params_update)
                            conn3.commit()
                            log_transaction("UPDATE", "Node 3", query_update, params_insert)

                    else:
                        # Update within the same node
                        print("SAD PATH")
                        if node_status["Node 1"]:
                            conn1_cursor.execute(query_update, params_update)
                            time.sleep(5)
                            conn1.commit()
                            st.info(f"Data updated into Node 1.")

                            # Depending on the year, log for replication to the backup node (Node 2 or Node 3)
                            if(node_status["Node 1"] and (node_status["Node 2"] == False or node_status["Node 3"]== False)):
                                backup_node = "Node 2" if updated_year < 2010 else "Node 3"
                                log_transaction("UPDATE_TEMP", backup_node, query_update, params_update)
                                st.warning(f"Node {backup_node} is unavailable. Will replicate update to {backup_node} once it comes back online")
                            else:    
                                backup_node = "Node 2" if updated_year <=2010 else "Node 3"
                                cursor = conn2_cursor if backup_node == "Node 2" else conn3_cursor
                                conn = conn2 if backup_node == "Node 2" else conn3
                                cursor.execute(query_update, params_update)
                                conn.commit()
                                log_transaction("UPDATE", backup_node, query_update, params_update)
                                st.success(f"Game successfully updated for {backup_node}.")
                        else:
                            backup_node = "Node 2" if updated_year <= 2010 else "Node 3"
                            cursor = conn2_cursor if backup_node == "Node 2" else conn3_cursor
                            conn = conn2 if backup_node == "Node 2" else conn3
                            cursor.execute(query_update, params_update)
                            time.sleep(5)
                            conn.commit()
                            log_transaction("UPDATE_TEMP", backup_node, query_update, params_update)
                            log_transaction("UPDATE_TEMP", "Node 1", query_update, params_update)
                            st.warning(f"Node 1 is unavailable. UPDATE applied to {backup_node} temporarily.")

                except Exception as e:
                    st.error(f"Error updating game: {e}")
                finally:
                    # Refresh Data
                    st.session_state.df = fetch_data(conn1, "SELECT * FROM games")


def delete():
    search_df = st.session_state.df
    st.header("Delete Game 🗑️")

    # Search for the game by ID or Name
    search_term = st.text_input("Search by Game ID or Name")
    search_results = search_df[
        (search_df["game_id"].astype(str) == search_term) | (search_df["name"].str.contains(search_term, case=False, na=False))
    ]

    if not search_results.empty:
        st.write("Search Results:")
        display_table(search_results)

        selected_id = st.number_input("Select Game ID to Delete", min_value=1, step=1)
        game_to_delete = df[df["game_id"] == selected_id]
        if not game_to_delete.empty:
            year = int(date_helper(game_to_delete.iloc[0]["release_date"]))  # Ensure year is an integer
            query = "DELETE FROM games WHERE game_id = %s"
            params = (selected_id,)

            if st.button("Delete"):
                try:
                    # Attempt deletion from Node 1
                    if node_status["Node 1"]:
                        conn1_cursor.execute(query, params)
                        time.sleep(5)
                        conn1.commit()
                        st.success(f"Game successfully deleted from Node 1.")

                        # Depending on the year, log for replication to the backup node (Node 2 or Node 3)
                        if(node_status["Node 1"] and (node_status["Node 2"] == False or node_status["Node 3"]== False)):
                                backup_node = "Node 2" if year < 2010 else "Node 3"
                                log_transaction("DELETE_TEMP", backup_node, query, params)
                                st.info(f"Will delete from {backup_node} once it comes back online NEW")
                        else:    
                            backup_node = "Node 2" if int(year) <= 2010 else "Node 3"
                            cursor = conn2_cursor if backup_node == "Node 2" else conn3_cursor
                            conn = conn2 if backup_node == "Node 2" else conn3
                            cursor.execute(query, params)
                            conn.commit()
                            log_transaction("DELETE", backup_node, query, params)
                            st.success(f"Game successfully deleted from {backup_node}.")
                        
                    else:
                        # Node 1 is unavailable, delete from backup node
                        backup_node = "Node 2" if int(year) <= 2010 else "Node 3"
                        cursor = conn2_cursor if backup_node == "Node 2" else conn3_cursor
                        conn = conn2 if backup_node == "Node 2" else conn3
                        cursor.execute(query, params)
                        time.sleep(5)
                        conn.commit()
                        log_transaction("DELETE_TEMP", backup_node, query, params)
                        log_transaction("DELETE_TEMP", "Node 1", query, params)
                        st.warning(f"Node 1 is unavailable. Delete applied to {backup_node} temporarily.")

                        # Optionally trigger replication from temporary logs to Node 1 later
                        if node_status["Node 1"]:
                            replicate_from_temp_logs_to_node_1()
                        # Update local DataFrame
                        st.session_state.df = fetch_data(conn1, "SELECT * FROM games")

                except Exception as e:
                    st.error(f"Error deleting game: {e}")
        else:
            st.warning("Game ID not found!")
    else:
        st.warning("No results found for your search.")


def report():
    st.header("Game Report 📊")
    report_df = st.session_state.df
    total_games = report_df.shape[0]

    report_df['year'] = pd.to_datetime(report_df['release_date'], errors='coerce').dt.year

    before_2010 = report_df[report_df['year'] < 2010].shape[0]
    after_2010 = report_df[report_df['year'] >= 2010].shape[0]

    st.write(f"The total number of games in the database is {total_games}")
    st.write(f"Games before 2010: {before_2010}")
    st.write(f"Games after 2010: {after_2010}")

    # Group by genre and display the count for each genre
    st.write("### Games by Platform")
    windows = report_df[report_df['windows'] == 1].shape[0]
    mac = report_df[report_df['mac'] == 1].shape[0]
    linux = report_df[report_df['linux'] == 1].shape[0]

    st.write(f"Windows: {windows}")
    st.write(f"Mac: {mac}")
    st.write(f"linux: {linux}")


def crash_simulation():
    # Add failure simulation toggle to the sidebar
    st.sidebar.header("Crash Simulation")
    for node in node_status.keys():
        node_status[node] = st.sidebar.checkbox(node, value=True)
    st.sidebar.checkbox("Simulate Failure in Node 1 Replication", key="simulate_failure_node_1")
    st.sidebar.checkbox("Simulate Failure in Node 2 or 3 Replication", key="simulate_failure_node_2or3")

def main():
    st.title("Steam Games Management 🎮")

    # Add crash simulation options to the sidebar
    crash_simulation()

    page = st.sidebar.radio("Select Operation", ["Show", "Search", "Insert", "Update", "Delete", "Report"])

    # Initialize or retain first selected node
    if 'first_selected_node' not in st.session_state:
        st.session_state.first_selected_node = None  # No node selected yet

    if page == "Insert":
        # Detect the first node click based on the checkbox states
        if node_status["Node 1"] and st.session_state.first_selected_node is None:
            st.session_state.first_selected_node = "Node 1"
            print("Node 1 is first clicked")

        elif node_status["Node 2"] and st.session_state.first_selected_node is None:
            st.session_state.first_selected_node = "Node 2"
            print("Node 2 is first clicked")

        elif node_status["Node 3"] and st.session_state.first_selected_node is None:
            st.session_state.first_selected_node = "Node 3"
            print("Node 3 is first clicked")

        insert()

    elif page == "Update":
        update()
    elif page == "Delete":
        delete()
    elif page == "Show":
        show()
    elif page == "Search":
        search()
    elif page == "Report":
        report()

    # Adjust the backup node based on the first selected node
    if st.session_state.first_selected_node == "Node 1" and (node_status["Node 2"] == True and node_status["Node 3"] == True):
        return
    elif st.session_state.first_selected_node == "Node 1" and (node_status["Node 2"] == True or node_status["Node 3"] == True):
        replicate_from_temp_logs_to_backup_node()
    elif (st.session_state.first_selected_node == "Node 2" or st.session_state.first_selected_node ==  "Node 3") and node_status["Node 1"] == True:
        replicate_from_temp_logs_to_node_1()

if __name__ == "__main__":
    main()
