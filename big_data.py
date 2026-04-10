import random
import csv

rows = 1_000_000  # 1 million rows

products = ["Laptop", "Mobile", "Tablet", "Headphones", "Smartwatch"]
countries = ["India", "USA", "UK", "Germany", "Canada"]

with open("sales_bigdata.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["order_id", "product", "price", "quantity", "country"])

    for i in range(rows):
        writer.writerow([
            i,
            random.choice(products),
            random.randint(100, 2000),
            random.randint(1, 5),
            random.choice(countries)
        ])

print("Big dataset created successfully!")
