import pandas as pd
import sqlite3

# Read in the data
df = pd.read_csv('AMAZON_daily.csv')

# Create a connection to the database
connection = sqlite3.connect('AMAZON.db')
cursor = connection.cursor()
# Drop the table if it already exists
cursor.execute('DROP TABLE IF EXISTS AMAZON')
# Write the data to a sqlite table
df.to_sql('AMAZON', connection)
# Close the connection
connection.close()
