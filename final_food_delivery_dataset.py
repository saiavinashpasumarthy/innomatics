import pandas as pd
import json
import sqlite3
from io import StringIO

# Step 1: Load CSV (orders)
orders = pd.read_csv('orders.csv')

# Step 2: Load JSON (users)
with open('users.json', 'r') as f:
    users = pd.DataFrame(json.load(f))

# Step 3: Load SQL (restaurants) - assuming CREATE TABLE + INSERT statements
with open('restaurants.sql', 'r') as f:
    sql_script = f.read()

conn = sqlite3.connect(':memory:')
conn.executescript(sql_script)
restaurants = pd.read_sql_query('SELECT * FROM restaurants', conn)  # Adjust table name if different
conn.close()

# Step 4-5: Left joins and save final CSV
merged = (orders
          .merge(users, on='user_id', how='left')
          .merge(restaurants, on='restaurant_id', how='left'))
merged.to_csv('final_food_delivery_dataset.csv', index=False)
print(merged.head())
print('Shape:', merged.shape)
