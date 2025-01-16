#Code of ETL operations on Country-GDP data

""" wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv """
#python3.11 -m pip install lxml
import pandas as pd
import requests 
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime 
import numpy as np

def log_progress(message):
    timestamp_format = "%Y-%h-%d-%H:%M:%S"
    time = datetime.now()
    time_stamp = time.strftime(timestamp_format)
    with open('code_log.txt', 'a') as l:
        l.write(time_stamp + ": " + message +'\n')

def extract(url, table_attribs):
    response = requests.get(url)
    if response.status_code == 200:
        html_content = response.text
        soup = BeautifulSoup(html_content, 'lxml')
    
        #table_element = soup.find('table', {'class': 'wikitable sortable mw-collapsible jquery-tablesorter mw-made-collapsible'})
        table_element = soup.select_one('#mw-content-text > div.mw-parser-output > table:nth-child(7)')
        rows = table_element.find_all('tr')
        data = []
        for row in rows:
            cells = row.find_all('td')
            #cell_text = [cell.get_text() for cell in cells]
            # Vérifier que la ligne a suffisamment de colonnes
            if len(cells) >= 3:
                selected_columns = [
                    cells[1].get_text(strip=True),  # Supposons que la colonne 1 contient une donnée pertinente
                    cells[2].get_text(strip=True),  # Supposons que la colonne 2 contient une donnée pertinente
                ]
                data.append(selected_columns)


        df = pd.DataFrame(data, columns=table_attribs)
        df['MC_USD_Billion'] = df['MC_USD_Billion'].apply(lambda x: float(x[:-1]))
        log_progress("Data extraction complete. Initiating Transformation process")
        return df
    else:
        print(f"Failed to retrieve the page. Status code: {response.status_code}")
        return None

def transform(df, csv_path):
    dataframe = pd.read_csv(csv_path)
    exchange_rate = dataframe.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate['INR'],2) for x in df['MC_USD_Billion']]
    log_progress("Data transformation complete. Initiating Loading process")
    return df

def load_to_csv(df, output_path='output.csv'):
    df.to_csv(output_path, index=False)
    log_progress("Data saved to CSV file")

def load_to_db(df, connection, table_name):
    df.to_sql(table_name, connection, if_exists='replace', index=False)
    connection.commit()
    log_progress("Data loaded to Database as a table, Executing queries")
    #connection.close()
    log_progress("Server Connection closed")


def run_query(query_statement, sql_connection):
    cursor = sql_connection.cursor()
    try:
        cursor.execute(query_statement)
        rows = cursor.fetchall()
        # Print the output of the query
        for row in rows:
            print(row)
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    log_progress("Process Complete")
    #sql_connection.close()
    #log_progress("Server Connection closed")

