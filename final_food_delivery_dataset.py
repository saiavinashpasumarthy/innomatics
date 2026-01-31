import pandas as pd
import json
import sqlite3

orders = pd.read_csv('orders.csv')
with open('users.json', 'r') as f:
    users = pd.DataFrame(json.load(f))
conn = sqlite3.connect(':memory:')
with open('restaurants.sql', 'r') as f:
    sql_script = f.read()
conn.executescript(sql_script)
restaurants = pd.read_sql_query('SELECT * FROM restaurants', conn)
conn.close()

merged = (orders
          .merge(users, on='user_id', how='left')
          .merge(restaurants, on='restaurant_id', how='left'))

merged['order_date'] = pd.to_datetime(merged['order_date'], dayfirst=True, errors='coerce')
merged['total_amount'] = pd.to_numeric(merged['total_amount'], errors='coerce')
hyd_revenue = round(merged[merged['city'] == 'Hyderabad']['total_amount'].sum())
gold_df = merged[merged['membership'] == 'Gold']
gold_aov = round(gold_df['total_amount'].mean(), 2)
top_combo = merged.groupby(['membership', 'cuisine'])['total_amount'].sum().idxmax()
high_rating_count = len(merged[merged['rating'] >= 4.5])
merged.to_csv('final_food_delivery_dataset.csv', index=False)

print(f"Merge Complete. Shape: {merged.shape}")
print("-" * 30)
print(f"Total Revenue (Hyderabad):  ${hyd_revenue}")
print(f"Average Order Value (Gold): ${gold_aov}")
print(f"Top Revenue Duo:            {top_combo}")
print(f"Orders with Rating >= 4.5:  {high_rating_count}")
print(f"Distinct Users in System:   {merged['user_id'].nunique()}")
