import pandas as pd
import requests
import boto3

class DataExtractor:
    def __init__(self):
        # You may initialize any common parameters or configurations here.
        pass

    def extract_data_from_csv(self, file_path):
        try:
            data = pd.read_csv(file_path)
            return data
        except Exception as e:
            print(f"Error extracting data from CSV: {e}")
            return None

    def extract_data_from_api(self, api_url):
        
        try:
            response = requests.get(api_url)
            data = response.json()
            return data
        except Exception as e:
            print(f"Error extracting data from API: {e}")
            return None

    def extract_data_from_s3(self, bucket_name, key):
        try:
            s3 = boto3.client('s3')
            response = s3.get_object(Bucket=bucket_name, Key=key)
            data = response['Body'].read()
            return data
        except Exception as e:
            print(f"Error extracting data from S3: {e}")
            return None
        


# %%
import yaml

class DatabaseConnector:
    def __init__(self, creds_file='db_creds.yaml'):
    
        self.creds_file = creds_file
        self.connection = None
        self.cursor = None
        self.db_creds = self.read_db_creds()

    def read_db_creds(self):
        try:
            with open(self.creds_file, 'r') as file:
                db_creds = yaml.safe_load(file)
            return db_creds
        except Exception as e:
            print(f"Error reading database credentials from YAML file: {e}")
            return None



#%%
import sqlite3
import yaml
from sqlalchemy import create_engine

class DatabaseConnector:
    def __init__(self, creds_file='db_creds.yaml'):
       
        self.creds_file = creds_file
        self.connection = None
        self.cursor = None
        self.db_creds = self.read_db_creds()
        self.db_engine = self.init_db_engine()

    def read_db_creds(self):
      
        try:
            with open(self.creds_file, 'r') as file:
                db_creds = yaml.safe_load(file)
            return db_creds
        except Exception as e:
            print(f"Error reading database credentials from YAML file: {e}")
            return None

    def init_db_engine(self):
       
        if self.db_creds is None:
            print("Database credentials not loaded. Cannot initialize the database engine.")
            return None

        try:
            # Assuming PostgreSQL, adjust the URL format based on your database type
            db_url = f"postgresql+psycopg2://{self.db_creds['RDS_USER']}:{self.db_creds['RDS_PASSWORD']}@{self.db_creds['RDS_HOST']}:{self.db_creds['RDS_PORT']}/{self.db_creds['RDS_DATABASE']}"
            engine = create_engine(db_url)
            print("Database engine initialized.")
            return engine
        except Exception as e:
            print(f"Error initializing the database engine: {e}")
            return None

    def connect_to_database(self):
       
        if self.db_creds is None or self.db_engine is None:
            print("Database credentials or engine not loaded. Cannot connect.")
            return

        try:
            self.connection = self.db_engine.connect()
            print(f"Connected to the database: {self.db_creds['RDS_DATABASE']}")
        except Exception as e:
            print(f"Error connecting to the database: {e}")

# %% step 4 
    def list_db_tables(self):
        """
        Lists all tables in the connected database.
        """
        if self.connection is None or self.db_engine is None:
            print("Database connection or engine not established. Cannot list tables.")
            return

        try:
            inspector = inspect(self.db_engine)
            tables = inspector.get_table_names()
            print("Tables in the database:")
            for table in tables:
                print(f"- {table}")
        except Exception as e:
            print(f"Error listing tables: {e}")

    def connect_to_database(self):
        """
        Establishes a connection to the database.
        """
        if self.db_creds is None or self.db_engine is None:
            print("Database credentials or engine not loaded. Cannot connect.")
            return

        try:
            self.connection = self.db_engine.connect()
            print(f"Connected to the database: {self.db_creds['RDS_DATABASE']}")
        except Exception as e:
            print(f"Error connecting to the database: {e}")

    def close_connection(self):
        """
        Closes the database connection.
        """
        if self.connection is not None:
            self.connection.close()
            print("Database connection closed.")

class DataExtractor:
    def __init__(self, db_connector):
        self.db_connector = db_connector

    def extract_data_from_db(self, table_name):
        """
        Extracts data from the specified table in the connected database.

        Parameters:
        - table_name (str): The name of the table in the database.

        Returns:
        - pd.DataFrame: A Pandas DataFrame containing the extracted data.
        """
        if self.db_connector.connection is None:
            print("Database connection not established. Cannot extract data.")
            return None

        try:
            query = f"SELECT * FROM {table_name}"
            data = pd.read_sql(query, self.db_connector.connection)
            return data
        except Exception as e:
            print(f"Error extracting data from the database: {e}")
            return None


#%%

import pandas as pd
import sqlite3
import yaml
from sqlalchemy import create_engine, inspect

class DatabaseConnector:
    def __init__(self, creds_file='db_creds.yaml'):
        """
        Initializes the DatabaseConnector with the given database credentials.

        Parameters:
        - creds_file (str): The path to the YAML file containing the database credentials.
        """
        self.creds_file = creds_file
        self.connection = None
        self.cursor = None
        self.db_creds = self.read_db_creds()
        self.db_engine = self.init_db_engine()

    def read_db_creds(self):
        """
        Reads database credentials from the YAML file.

        Returns:
        - dict: A dictionary containing the database credentials.
        """
        try:
            with open(self.creds_file, 'r') as file:
                db_creds = yaml.safe_load(file)
            return db_creds
        except Exception as e:
            print(f"Error reading database credentials from YAML file: {e}")
            return None

    def init_db_engine(self):
        """
        Initializes and returns an SQLAlchemy database engine.

        Returns:
        - sqlalchemy.engine.Engine: An SQLAlchemy database engine.
        """
        if self.db_creds is None:
            print("Database credentials not loaded. Cannot initialize the database engine.")
            return None

        try:
            # Assuming PostgreSQL, adjust the URL format based on your database type
            db_url = f"postgresql+psycopg2://{self.db_creds['RDS_USER']}:{self.db_creds['RDS_PASSWORD']}@{self.db_creds['RDS_HOST']}:{self.db_creds['RDS_PORT']}/{self.db_creds['RDS_DATABASE']}"
            engine = create_engine(db_url)
            print("Database engine initialized.")
            return engine
        except Exception as e:
            print(f"Error initializing the database engine: {e}")
            return None

    def list_db_tables(self):
        """
        Lists all tables in the connected database.
        """
        if self.connection is None or self.db_engine is None:
            print("Database connection or engine not established. Cannot list tables.")
            return

        try:
            inspector = inspect(self.db_engine)
            tables = inspector.get_table_names()
            print("Tables in the database:")
            for table in tables:
                print(f"- {table}")
        except Exception as e:
            print(f"Error listing tables: {e}")

    def connect_to_database(self):
        """
        Establishes a connection to the database.
        """
        if self.db_creds is None or self.db_engine is None:
            print("Database credentials or engine not loaded. Cannot connect.")
            return

        try:
            self.connection = self.db_engine.connect()
            print(f"Connected to the database: {self.db_creds['RDS_DATABASE']}")
        except Exception as e:
            print(f"Error connecting to the database: {e}")

    def close_connection(self):
        """
        Closes the database connection.
        """
        if self.connection is not None:
            self.connection.close()
            print("Database connection closed.")

class DataExtractor:
    def __init__(self, db_connector):
        self.db_connector = db_connector

    def extract_data_from_db(self, table_name):
        """
        Extracts data from the specified table in the connected database.

        Parameters:
        - table_name (str): The name of the table in the database.

        Returns:
        - pd.DataFrame: A Pandas DataFrame containing the extracted data.
        """
        if self.db_connector.connection is None:
            print("Database connection not established. Cannot extract data.")
            return None

        try:
            query = f"SELECT * FROM {table_name}"
            data = pd.read_sql(query, self.db_connector.connection)
            return data
        except Exception as e:
            print(f"Error extracting data from the database: {e}")
            return None

    def read_rds_table(self, table_name):
        """
        Reads a specified table from the RDS database and returns a Pandas DataFrame.

        Parameters:
        - table_name (str): The name of the table in the RDS database.

        Returns:
        - pd.DataFrame: A Pandas DataFrame containing the table data.
        """
        if self.db_connector.connection is None:
            print("Database connection not established. Cannot read RDS table.")
            return None

        try:
            data = self.extract_data_from_db(table_name)
            return data
        except Exception as e:
            print(f"Error reading RDS table: {e}")
            return None

#%% step 7

import sqlite3
import yaml
from sqlalchemy import create_engine

class DatabaseConnector:
    def __init__(self, creds_file='db_creds.yaml'):
        """
        Initializes the DatabaseConnector with the given database credentials.

        Parameters:
        - creds_file (str): The path to the YAML file containing the database credentials.
        """
        self.creds_file = creds_file
        self.connection = None
        self.cursor = None
        self.db_creds = self.read_db_creds()
        self.db_engine = self.init_db_engine()

    def read_db_creds(self):
        """
        Reads database credentials from the YAML file.

        Returns:
        - dict: A dictionary containing the database credentials.
        """
        try:
            with open(self.creds_file, 'r') as file:
                db_creds = yaml.safe_load(file)
            return db_creds
        except Exception as e:
            print(f"Error reading database credentials from YAML file: {e}")
            return None

    def init_db_engine(self):
        """
        Initializes and returns an SQLAlchemy database engine.

        Returns:
        - sqlalchemy.engine.Engine: An SQLAlchemy database engine.
        """
        if self.db_creds is None:
            print("Database credentials not loaded. Cannot initialize the database engine.")
            return None

        try:
            # Assuming PostgreSQL, adjust the URL format based on your database type
            db_url = f"postgresql+psycopg2://{self.db_creds['RDS_USER']}:{self.db_creds['RDS_PASSWORD']}@{self.db_creds['RDS_HOST']}:{self.db_creds['RDS_PORT']}/{self.db_creds['RDS_DATABASE']}"
            engine = create_engine(db_url)
            print("Database engine initialized.")
            return engine
        except Exception as e:
            print(f"Error initializing the database engine: {e}")
            return None

    def list_db_tables(self):
        """
        Lists all tables in the connected database.
        """
        if self.connection is None or self.db_engine is None:
            print("Database connection or engine not established. Cannot list tables.")
            return

        try:
            inspector = inspect(self.db_engine)
            tables = inspector.get_table_names()
            print("Tables in the database:")
            for table in tables:
                print(f"- {table}")
        except Exception as e:
            print(f"Error listing tables: {e}")

    def connect_to_database(self):
        """
        Establishes a connection to the database.
        """
        if self.db_creds is None or self.db_engine is None:
            print("Database credentials or engine not loaded. Cannot connect.")
            return

        try:
            self.connection = self.db_engine.connect()
            print(f"Connected to the database: {self.db_creds['RDS_DATABASE']}")
        except Exception as e:
            print(f"Error connecting to the database: {e}")

    def upload_to_db(self, dataframe, table_name):
        """
        Uploads a Pandas DataFrame to the specified table in the connected database.

        Parameters:
        - dataframe (pd.DataFrame): The Pandas DataFrame to upload.
        - table_name (str): The name of the table in the database.

        Returns:
        - bool: True if the upload is successful, False otherwise.
        """
        if self.connection is None:
            print("Database connection not established. Cannot upload data.")
            return False

        try:
            dataframe.to_sql(table_name, self.connection, index=False, if_exists='replace')
            print(f"Data uploaded to the table '{table_name}' in the database.")
            return True
        except Exception as e:
            print(f"Error uploading data to the database: {e}")
            return False

    def close_connection(self):
        """
        Closes the database connection.
        """
        if self.connection is not None:
            self.connection.close()
            print("Database connection closed.")

#%% 

# Example usage:
connector = DatabaseConnector()
connector.connect_to_database()

# Assuming cleaned_user_data is obtained from previous steps
cleaned_user_data = cleaning.clean_user_data(user_data)
connector.upload_to_db(cleaned_user_data, 'dim_users')

connector.close_connection()


#%%

import requests

class DataExtractor:
    def __init__(self, db_connector):
        self.db_connector = db_connector

    def list_number_of_stores(self, endpoint, headers):
        """
        Returns the number of stores to extract from the API.

        Parameters:
        - endpoint (str): The API endpoint URL for getting the number of stores.
        - headers (dict): The headers to include in the API request.

        Returns:
        - int: The number of stores to extract.
        """
        try:
            response = requests.get(endpoint, headers=headers)
            response.raise_for_status()

            num_stores = response.json().get('num_stores', 0)
            return num_stores
        except requests.exceptions.RequestException as e:
            print(f"Error fetching number of stores from API: {e}")
            return 0

#%%

import requests
import pandas as pd

class DataExtractor:
    def __init__(self, db_connector):
        self.db_connector = db_connector

    def list_number_of_stores(self, endpoint, headers):
    
        try:
            response = requests.get(endpoint, headers=headers)
            response.raise_for_status()

            # Assuming the API response contains the number of stores as a key 'num_stores'
            num_stores = response.json().get('num_stores', 0)
            return num_stores
        except requests.exceptions.RequestException as e:
            print(f"Error fetching number of stores from API: {e}")
            return 0

    def retrieve_stores_data(self, endpoint, headers):
       
        try:
            response = requests.get(endpoint, headers=headers)
            response.raise_for_status()

            # Assuming the API response contains a list of stores under the key 'stores'
            stores_data = response.json().get('stores', [])

            # Convert the list of stores to a Pandas DataFrame
            stores_df = pd.DataFrame(stores_data)

            return stores_df
        except requests.exceptions.RequestException as e:
            print(f"Error fetching stores data from API: {e}")
            return pd.DataFrame()


