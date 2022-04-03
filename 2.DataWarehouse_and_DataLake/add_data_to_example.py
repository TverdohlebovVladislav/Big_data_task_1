import pandas as pd
import os
import crontab
from numpy import random as np_rand
import datetime
import pathlib

path = str(pathlib.Path().resolve())
print(__file__ + '/../')

# --- 
# Variables
ext = ".csv"

customer_table_name = "customer"
payments_table_name = "payments"
customer_totals_name = "customer_totals"

SOURCE = "~/project/2.DataWarehouse_and_DataLake/example"

customer_df = pd.read_csv(os.path.join(SOURCE,customer_table_name+ext))
payments_df = pd.read_csv(os.path.join(SOURCE,payments_table_name+ext)) 

# ---
# Generate data
f_names = ['Liam', 'Jackson', 'Noah', 'Lucas', 'Oliver', 'Grayson', 'Leo', 'Jack', 'Benjamin', 'William', 'Luca',
            'Logan', 'Ethan', 'Levi', 'James', 'Henry', 'Mateo', 'Jacob', 'Elliot', 'Mason', 'Miles', 'Theodore',
            'Nathan', 'Owen', 'Alexander', 'Olivia', 'Emma', 'Mia', 'Sophia', 'Zoey', 'Charlotte', 'Amelia', 'Aria', 'Mila', 'Hannah', 'Ava', 'Chloe',
            'Ella', 'Abigail', 'Everly', 'Leah', 'Nora', 'Ellie', 'Isabella', 'Riley', 'Avery', 'Lily', 'Isla',
            'Scarlett',
            'Charlie']
l_names = ['Anderson', 'Beaulieu', 'Belanger', 'Bouchard', 'Cameron', 'Campbell', 'Chen', 'Cote', 'Davis', 'Evans',
        'Fortin', 'Gagnon',
        'Gauthier', 'Gill', 'Girard', 'Graham', 'Grant', 'Hall', 'Harris', 'Jackson', 'Johnson', 'Johnston',
        'Kennedy', 'Khan', 'King',
        'Landry', 'Lapointe', 'Lavoie', 'Leblanc', 'Levesque', 'Lewis', 'Lin', 'MacDonald', 'Martin', 'Mitchell',
        'Michaud', 'Morin',
        'Morrison', 'Murphy', 'Murray', 'Nadeau', 'Nguyen', 'Ouellet', 'Patel', 'Pelletier', 'Peters', 'Reynolds',
        'Richard', 'Rogen', 'Robinson']
tariffs = ['green', 'red', 'yellow', 'blue']

for i in range(np_rand.randint(2, 5)):
    index = len(customer_df['customer_id']) + 1 if i == 0 else index + 1
    f_name = np_rand.choice(f_names) +  ' ' + np_rand.choice(l_names)
    tarif = np_rand.choice(tariffs)
    new_row = {'customer_id': index, 'full_name': f_name, 'tariff': tarif}
    customer_df = customer_df.append(new_row, ignore_index=True)
    # customer_df['customer_id'][index] = index

for i in range(np_rand.randint(2, 20)):
    index = len(payments_df['payment_id']) + 1 if i == 0 else index + 1
    customer_id = np_rand.randint(1, len(customer_df['customer_id']))
    payment_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    amount = np_rand.randint(40, 300)
    new_row = {'payment_id': index, 'customer_id': customer_id, 'payment_timestamp': payment_timestamp, 'amount': amount}
    payments_df = payments_df.append(new_row, ignore_index=True)


customer_df.set_index('customer_id', inplace=True)
payments_df.set_index('payment_id', inplace=True)
customer_df.to_csv(os.path.join(SOURCE, customer_table_name + ext))
payments_df.to_csv(os.path.join(SOURCE, payments_table_name + ext))
