import csv
import random

rows = 1500
categories = ["electronics", "books", "clothing", "home", "sports"]

with open("sample_sales.csv", mode="w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["transaction_id", "category", "amount"])
    for i in range(1, rows + 1):
        writer.writerow([i, random.choice(categories), round(random.uniform(5.0, 500.0), 2)])

print(f"Generated sample_sales.csv with {rows} records.")