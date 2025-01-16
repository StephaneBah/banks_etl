from banks_project import log_process, extract, transform, load_to_csv, load_to_db, run_query

''' Here, we define the required entities and call the relevant
functions in the correct order to complete the project.'''

log_progress("Preliminaries complete. Initiating ETL process")
df = extract("https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks", table_attribs=['Name', 'MC_USD_Billion'])

print("------After Extraction------\n")
print(df)
print("------------\n")

df = transform(df, 'exchange_rate.csv')

print("------After Transformation------\n")
print(df)
print("------------\n")

load_to_csv(df)

conn = sqlite3.connect('Banks.db')
log_progress("SQL Connection initiated")
load_to_db(df, conn, 'Largest_banks')

print("Query 'SELECT * FROM Largest_banks':")
run_query("SELECT * FROM Largest_banks", conn)
print("------------\n")

print("Query 'SELECT AVG(MC_GBP_Billion) FROM Largest_banks':")
run_query("SELECT AVG(MC_GBP_Billion) FROM Largest_banks", conn)
print("------------\n")

print("Query 'SELECT Name from Largest_banks LIMIT 5':")
run_query("SELECT Name from Largest_banks LIMIT 5", conn)
print("------------\n")

conn.close()
log_progress("Server Connection closed")
