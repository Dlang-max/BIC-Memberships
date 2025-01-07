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
try:
    database_connection = mysql.connector.connect(
        host=HOST,
        database=DATABASE_NAME,
        user=USER,
        password=PASSWORD
    )
except mysql.connector.Error as err:
    print("Error Connecting to MySQL DB")
    exit()

# Establish connection to Google Sheets
scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive.file', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('./key.json', scope)
client = gspread.authorize(creds)
sheet = client.open('Buffalo Irish Center Active Members')
sheet = sheet.worksheet('Active Members')

# SQL query to get the first and last name of users with active subscriptions
query = """
SELECT 
    users.ID AS user_id,
    meta_first.meta_value AS first_name,
    meta_last.meta_value AS last_name,
    users.user_email AS email,
    CONCAT_WS(', ', 
        meta_address_one.meta_value, 
        meta_address_two.meta_value, 
        meta_address_city.meta_value, 
        meta_address_state.meta_value, 
        meta_address_zip.meta_value, 
        meta_address_country.meta_value
    ) AS full_address,
    subs.status
FROM 
    jqo_mepr_subscriptions AS subs
INNER JOIN 
    jqo_users AS users 
    ON subs.user_id = users.ID
INNER JOIN 
    jqo_usermeta AS meta_first 
    ON subs.user_id = meta_first.user_id AND meta_first.meta_key = 'first_name'
INNER JOIN 
    jqo_usermeta AS meta_last 
    ON subs.user_id = meta_last.user_id AND meta_last.meta_key = 'last_name'
LEFT JOIN 
    jqo_usermeta AS meta_address_one 
    ON subs.user_id = meta_address_one.user_id AND meta_address_one.meta_key = 'mepr-address-one'
LEFT JOIN 
    jqo_usermeta AS meta_address_two 
    ON subs.user_id = meta_address_two.user_id AND meta_address_two.meta_key = 'mepr-address-two'
LEFT JOIN 
    jqo_usermeta AS meta_address_city 
    ON subs.user_id = meta_address_city.user_id AND meta_address_city.meta_key = 'mepr-address-city'
LEFT JOIN 
    jqo_usermeta AS meta_address_state 
    ON subs.user_id = meta_address_state.user_id AND meta_address_state.meta_key = 'mepr-address-state'
LEFT JOIN 
    jqo_usermeta AS meta_address_zip 
    ON subs.user_id = meta_address_zip.user_id AND meta_address_zip.meta_key = 'mepr-address-zip'
LEFT JOIN 
    jqo_usermeta AS meta_address_country 
    ON subs.user_id = meta_address_country.user_id AND meta_address_country.meta_key = 'mepr-address-country'
WHERE 
    subs.status = 'active'
ORDER BY 
    meta_last.meta_value ASC;
"""

try: 
    # The cursor allows use to process information from the mySQL database one
    # row at a time. 
    cursor = database_connection.cursor()
    cursor.execute(query)

    data = []
    for (_, first_name, last_name, email, address, status) in cursor:
        print(f"{first_name} {last_name} {email} {address} {status}")
        data.append([first_name, last_name, email, address, status])

    # Update the Google Sheet:
    # Using pandas DataFrame instead of repeatedly querying and updating
    # Google Sheet. Want to avoid getting rate limited.
    df = pd.DataFrame(data=data, columns=["First Name", "Last Name", "Email", "Address", "Membership Status"])
    sheet.clear()
    sheet.update([df.columns.values.tolist()] + df.values.tolist())
except Exception as e:
    print("Error updating Google Sheet")
    exit()
finally:
    cursor.close()
    database_connection.close()