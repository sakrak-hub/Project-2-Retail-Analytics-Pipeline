from data_generator_2 import RetailDataGenerator
from datetime import datetime

transaction_date = datetime(2026,3,21)

retail_generator = RetailDataGenerator(folder_path='/mnt/d/Projects/Project-2-Retail-Analytics-Pipeline/tmp/master_data')

data = retail_generator.generate_daily_transactions(transaction_date)

print("Print Keys")
for i in data[0].keys():
    print(i)
print("\n")
print("Print Values")
for j in data[0].values():
    print(j)