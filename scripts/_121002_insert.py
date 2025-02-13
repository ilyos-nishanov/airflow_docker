
from datetime import datetime
from _121002_utils import generate_date_ranges
from multiprocessing import Pool, cpu_count
from _121002_fetch_and_write_data import fetch_and_write_data

print(f"Start time: {datetime.now()}")
table_name = '[BRONZE].[121002_test]'
date_ranges = generate_date_ranges(2023, 2025, 'month')

with Pool(cpu_count()) as pool:
    tasks = [(date_range, table_name) for date_range in date_ranges]
    results = pool.starmap(fetch_and_write_data, tasks)

print(f"End time: {datetime.now()}")
print("Results:", results)