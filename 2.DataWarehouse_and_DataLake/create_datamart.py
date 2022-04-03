import pandas as pd
import os
import datetime
from pandasql import sqldf

# ---
# Variables
ext = ".csv"
NOW_DATE = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
LAST_UPLOAD_DATE = ''

customer_table_name = "customer"
payments_table_name = "payments"
customer_totals_name = "customer_totals"

SOURCE = "~/project/2.DataWarehouse_and_DataLake/example"
RAW = "/home/vlad/project/2.DataWarehouse_and_DataLake/raw"
DATAMART = "/home/vlad/project/2.DataWarehouse_and_DataLake/datamart"

customer_df = pd.read_csv(os.path.join(SOURCE,customer_table_name+ext))
payments_df = pd.read_csv(os.path.join(SOURCE,payments_table_name+ext)) 

def add_date_time_upl(df: pd.DataFrame) -> pd.DataFrame:
    df['date_of_upload'] = [NOW_DATE for i in range(len(df.index))]
    return df

# ---
# Create RAW
if not os.path.exists(RAW):
    customer_df = add_date_time_upl(customer_df)
    payments_df = add_date_time_upl(payments_df)
    os.mkdir(RAW)
else:
    customer_df_raw = pd.read_csv(os.path.join(RAW,customer_table_name+ext))
    payments_df_raw = pd.read_csv(os.path.join(RAW,payments_table_name+ext)) 
    LAST_UPLOAD_DATE = list(payments_df_raw['date_of_upload'])[-1]

    customer_df = customer_df[customer_df.index > len(customer_df_raw.index) - 1]
    payments_df = payments_df[payments_df.index > len(payments_df_raw.index) - 1]

    customer_df = add_date_time_upl(customer_df)
    payments_df = add_date_time_upl(payments_df)

    customer_df = pd.concat([customer_df_raw, customer_df], ignore_index=True)
    payments_df = pd.concat([payments_df_raw, payments_df], ignore_index=True)
    pass

customer_df.set_index('customer_id', inplace=True)
payments_df.set_index('payment_id', inplace=True)
customer_df.to_csv(os.path.join(RAW,customer_table_name+ext))
payments_df.to_csv(os.path.join(RAW,payments_table_name+ext))


# ---
# Calculate datamart
customer_df = pd.read_csv(os.path.join(RAW,customer_table_name+ext))
payments_df = pd.read_csv(os.path.join(RAW,payments_table_name+ext))

pysqldf = lambda q: sqldf(q, globals())

customer_total_df = pysqldf("""
                    select c.customer_id, sum(p.amount) as amount 
                    from customer_df as c
                    join payments_df as p on c.customer_id=p.customer_id 
                    WHERE p.date_of_upload BETWEEN '""" + LAST_UPLOAD_DATE + "' AND '" + NOW_DATE + """'
                    group by c.customer_id
                    """)

if not os.path.exists(DATAMART):
    os.mkdir(DATAMART)

customer_total_df.to_csv(os.path.join(DATAMART,customer_totals_name+ext))
customer_total_df.set_index('customer_id')