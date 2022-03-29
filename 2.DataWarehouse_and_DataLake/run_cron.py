from crontab import CronTab

cron = CronTab(user=True)

# ---
#  Task of upload data to SOURCE 
# job0 = cron.new(command='source ~/project/venv/bin/activate')
job1 = cron.new(command='~/project/venv/bin/python ~/project/2.DataWarehouse_and_DataLake/add_data_to_example.py')
# job0.minute.every(1)
job1.minute.every(1)
# job1.second.every(10)

# ---
#  Task of upload new data to RAW and create data mart 


# -------------Write 
cron.write()
# print (job0.is_valid())
print (job1.is_valid())
