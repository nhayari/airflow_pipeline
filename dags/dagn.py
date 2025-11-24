from airflow.models.dag import dag
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task
import requests
from requests.auth import HTTPBasicAuth
import json
import duckdb

URL_SKY = "https://opensky-network.org/api/states/all?extended=true"
URL_TOKEN = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
CLIENT_ID = "olivierwarda@gmail.com-api-client"
CLIENT_SECRET = "foUbou4vXSVqIhYPkwwzQVWISyIWNnBN"
cols = [
            "icao24",
            "callsign",
            "origin_country",
            "time_position",
            "last_contact",
            "longitude",
            "latitude",
            "baro_altitude",
            "on_ground",
            "velocity",
            "true_track",
            "vertical_rate",
            "sensors",
            "geo_altitude",
            "squawk",
            "spi",
            "position_source",
            "category",
        ]
DAT_FILE_NAME = "/opt/airflow/dags/data/data.json"


'''
Get access token from OpenSky API
'''
def getaccess_token(tokenurl,clientId=CLIENT_ID,clientSecret=CLIENT_SECRET):
    if not CLIENT_ID or not CLIENT_SECRET:
        print("Error: OPENSKY_CLIENT_ID and OPENSKY_CLIENT_SECRET must be set in Colab Secrets.")
        access_token = None
    else:
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {
            "grant_type": "client_credentials",
            "client_id": clientId,
            "client_secret": clientSecret
        }

        access_token = None
        try:
            token_response = requests.post(tokenurl, headers=headers, data=data)
            token_response.raise_for_status() # Raise an exception for HTTP errors
            token_data = token_response.json()
            access_token = token_data.get("access_token")
            print("Access token obtained successfully.")
        except requests.exceptions.HTTPError as e:
            print(f"HTTP Error while getting token: {e.response.status_code} - {e.response.reason}")
            if e.response.content:
                print(e.response.json())
        except Exception as e:
            print(f"An error occurred while getting token: {e}")
    return access_token

'''
Get flight data from OpenSky API
'''
def get_flight_data(colonnes,url,file_data_name):
    access_token = getaccess_token(URL_TOKEN)
    if access_token:
        auth_headers = {"Authorization": f"Bearer {access_token}"}
        try:
            resp = requests.get(url, headers=auth_headers)
            resp.raise_for_status() # Raise an exception for HTTP errors
            flight_data_raw = resp.json() # Renamed to avoid confusion with processed data
            print("Successfully fetched raw flight data using Bearer token.")

            # Process the raw flight data into a list of dictionaries
            timesf = flight_data_raw.get('time')
            flights_states = flight_data_raw.get('states', [])

            # yes merge
            flights_states_extended = [dict(zip(colonnes, flight)) for flight in flights_states]
            flight_data = {"timestamp": timesf, "flights": flights_states_extended}

            print(f"Timestamp of data: {flight_data['timestamp']}")
            print(f"Number of flight states received: {len(flight_data['flights'])}")
            # Display first entries for logs
            if flight_data['flights']:
                print("First 3 flight entries:")
                for i in range(min(3, len(flight_data['flights']))):
                    print(flight_data['flights'][i])

            # Save the processed flight data to a JSON file
            with open(file_data_name, 'w') as f:
                json.dump(flights_states_extended, f, indent=4)         

        except requests.exceptions.HTTPError as e:
            print(f"HTTP Error while fetching flight data: {e.response.status_code} - {e.response.reason}")
            if e.response.content:
                print(e.response.json())
        except Exception as e:
            print(f"An error occurred while fetching flight data: {e}")
    else:
        print("No access token available. Cannot fetch flight data.")





@task()
def load_data():
    get_flight_data(cols,URL_SKY,DAT_FILE_NAME)

@task()
def check_nb():
    print("how many number!")

@task()
def check_duplicate():
    print("how many duplication!")    


def save_dataooooooooo(file_name=DAT_FILE_NAME):
    con = duckdb.connect('dags/data/bdd_flight')
    con.sql(f"INSERT INTO TABLE bd_flight.main.openskynetwork_brute SELECT * FROM '{file_name}'");     
    

DB_PATH = "/opt/airflow/dags/data/bd_flight"



@task()
def save_data():
    DB_PATH = "/opt/airflow/dags/data/bdd_flight.duckdb"

    con = duckdb.connect(DB_PATH, read_only=False)
    con.sql("""
        CREATE SCHEMA IF NOT EXISTS bdd_flight.main;
    """)

    con.sql(f"""
        CREATE TABLE IF NOT EXISTS bdd_flight.main.openskynetwork_brute AS 
        SELECT * FROM read_json_auto('{DAT_FILE_NAME}') LIMIT 0;
    """)

    con.sql(f"""
        INSERT INTO bdd_flight.main.openskynetwork_brute
        SELECT * FROM read_json_auto('{DAT_FILE_NAME}');
    """)




@dag()
def my_sample_dag():
    (
    EmptyOperator(task_id='start')
    >> load_data()
    >> save_data()
    >> [check_nb(), check_duplicate()]
    >> EmptyOperator(task_id='end')
    )

dag_instance = my_sample_dag()