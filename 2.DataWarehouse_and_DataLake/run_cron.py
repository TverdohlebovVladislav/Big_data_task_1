from crontab import CronTab

cron = CronTab(user=True)

# ---
#  Task of upload data to SOURCE 
job1 = cron.new(command='~/project/venv/bin/python ~/project/2.DataWarehouse_and_DataLake/add_data_to_example.py')
job2 = cron.new(command='~/project/venv/bin/python ~/project/2.DataWarehouse_and_DataLake/create_datamart.py')

job1.minute.every(1)
job2.minute.every(2)


# -------------Write 
cron.write()

print (job1.is_valid())
print (job2.is_valid())