# Code for ETL operations on Country-GDP data

# Importing the required libraries
import requests
import pandas as pd
import sqlite3
import numpy as np
import logging
from bs4 import BeautifulSoup
from datetime import datetime

# Initialize logging
logging.basicConfig(filename='code_log.txt', level=logging.INFO, format='%(message)s')

# Known variables
DATA_URL = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
EXCHANGE_RATE_CSV_PATH = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv"
OUTPUT_CSV_PATH = './Largest_banks_data.csv'
DATABASE_NAME = 'Banks.db'
TABLE_NAME = 'Largest_banks'

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing.'''
    time_stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"{time_stamp} : {message}"
    logging.info(log_entry)  # Use logging to write the message

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    
    # Fetch the webpage
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Locate the table under the heading "By market capitalization"
    table = soup.find_all('table')[0]  # Assuming the first table is the required one
    rows = table.find_all('tr')[1:]  # Skip the header row

    # Extract data
    bank_data = []
    for row in rows:
        cols = row.find_all('td')
        bank_name = cols[1].text.strip()
        market_cap = float(cols[2].text.strip().replace(',', '').replace('\n', ''))  # Remove comma and newline, convert to float
        bank_data.append([bank_name, market_cap])

    # Create DataFrame
    df = pd.DataFrame(bank_data, columns=['Bank Name', 'Market Cap (US$ billion)'])
    return df

# Function call
df_banks = extract(DATA_URL, ['Bank Name', 'Market Cap (US$ billion)'])
print(df_banks)

# Log the progress
log_progress("Data extraction complete. Initiating Transformation process")

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies.'''
    
    # Read the exchange rate CSV file
    exchange_rate_df = pd.read_csv(csv_path)
    
    # Convert the contents to a dictionary
    exchange_rate = exchange_rate_df.set_index('Currency').to_dict()['Rate']
    
    # Add columns for market cap in different currencies
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['Market Cap (US$ billion)']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['Market Cap (US$ billion)']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['Market Cap (US$ billion)']]
    
    return df

# Function call
df_banks_transformed = transform(df_banks, EXCHANGE_RATE_CSV_PATH)

# Comment out previous print statements
# print(df_banks)

# Print the transformed DataFrame
print(df_banks_transformed)

# Log the progress
log_progress("Data transformation complete. Initiating Loading process")

# Print the market capitalization of the 5th largest bank in billion EUR
print(df_banks_transformed['MC_EUR_Billion'][4])

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path, index=False)
    log_progress("Data saved to CSV file")  # Log the action

# Load the transformed DataFrame to a CSV file
load_to_csv(df_banks_transformed, OUTPUT_CSV_PATH)

# Uncomment if you want to see the DataFrame in the terminal
# print(df_banks_transformed)

# Log entry is already handled in the load_to_csv functions$4    

def load_to_db(conn, table_name, df):
    ''' This function loads the DataFrame into the specified SQL database table.
    The function returns nothing.'''
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    log_progress("Data loaded to Database as a table, Executing queries")  # Log the action

 # Establish connection to SQLite3 database
conn = sqlite3.connect(DATABASE_NAME)

# Load the transformed DataFrame to the database
load_to_db(conn, TABLE_NAME, df_banks_transformed)

# Close the connection
conn.close()

# Log entry is already handled in the load_to_db function   

def run_queries(conn, query):
    ''' This function executes the provided SQL query using the given connection
    and prints the query and its results.'''
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    
    # Print the query and its output
    print(f"Query: {query}")
    for row in result:
        print(row)
    
    # Log the query execution
    log_progress(f"Executed query: {query}")

# Establish connection to SQLite3 database
conn = sqlite3.connect(DATABASE_NAME)

# Check the structure of the table
run_queries(conn, "PRAGMA table_info(Largest_banks)")

# Execute the queries
run_queries(conn, "SELECT * FROM Largest_banks")
run_queries(conn, "SELECT AVG(MC_GBP_Billion) FROM Largest_banks")
run_queries(conn, "SELECT `Bank Name` FROM Largest_banks LIMIT 5")  # Adjust based on actual column name

# Close the connection
conn.close()