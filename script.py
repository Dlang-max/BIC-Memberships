import os
import gspread
import pandas as pd
import mysql.connector
from oauth2client.service_account import ServiceAccountCredentials

HOST = os.getenv('HOST')
DATABASE_NAME = os.getenv('DATABASE_NAME')
USER = os.getenv('USER')
PASSWORD = os.getenv('PASSWORD')

# Establish database connection
database_connection = mysql.connector.connect(
    host=HOST,
    database=DATABASE_NAME,
    user=USER,
    password=PASSWORD
)

# Establish connection to Google Sheets
scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive.file', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('./key.json', scope)
client = gspread.authorize(creds)
sheet = client.open('Buffalo Irish Center Active Members')
sheet = sheet.worksheet('Active Members')

cursor = database_connection.cursor()

# SQL query to get the first and last name of users with active subscriptions
query = """
SELECT 
    meta_first.user_id,
    meta_first.meta_value AS first_name,
    meta_last.meta_value AS last_name,
    subs.status
FROM 
    jqo_mepr_subscriptions AS subs
INNER JOIN 
    jqo_usermeta AS meta_first 
    ON subs.user_id = meta_first.user_id AND meta_first.meta_key = 'first_name'
INNER JOIN 
    jqo_usermeta AS meta_last 
    ON subs.user_id = meta_last.user_id AND meta_last.meta_key = 'last_name'
WHERE 
    subs.status = 'active'
ORDER BY 
    meta_last.meta_value ASC;
"""

# Execute the query
cursor.execute(query)

# Update the Google Sheet
df = pd.DataFrame(columns=["First Name", "Last Name", "Status"])
for idx, (user_id, first_name, last_name, status) in enumerate(cursor):
    df.loc[len(df)] = [first_name, last_name, status]
    print(f"{first_name} {last_name} -> {status}")
sheet.update([df.columns.values.tolist()] + df.values.tolist())

database_connection.close()