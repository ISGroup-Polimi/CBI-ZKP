# Star Schema Generator

import pandas as pd
import os
import numpy as np
from datetime import datetime

from Org1.Dim_ID_Converter import START_DATE # ="2020-01-01"

os.makedirs("Org1/PR_DB/DimTab", exist_ok=True)

SALE_COUNTER = 0

def product_Gen(output_dir="Org1/PR_DB/DimTab"):
    # Define 4 products for each of the 3 categories
    products = [
        # Shoes
        {"Product_Id": 1, "Product_Name": "Running Shoes", "Category": "Shoes"},
        {"Product_Id": 2, "Product_Name": "Leather Boots", "Category": "Shoes"},
        {"Product_Id": 3, "Product_Name": "Slip-On Sneakers", "Category": "Shoes"},
        {"Product_Id": 4, "Product_Name": "Sandals", "Category": "Shoes"},
        # Pants
        {"Product_Id": 5, "Product_Name": "Denim Jeans", "Category": "Pants"},
        {"Product_Id": 6, "Product_Name": "Cargo Pants", "Category": "Pants"},
        {"Product_Id": 7, "Product_Name": "Chino Pants", "Category": "Pants"},
        {"Product_Id": 8, "Product_Name": "Shorts", "Category": "Pants"},
        # Shirts
        {"Product_Id": 9, "Product_Name": "Classic T-Shirt", "Category": "Shirts"},
        {"Product_Id": 10, "Product_Name": "Formal Shirt", "Category": "Shirts"},
        {"Product_Id": 11, "Product_Name": "Polo Shirt", "Category": "Shirts"},
        {"Product_Id": 12, "Product_Name": "Tank Top", "Category": "Shirts"},
    ]
    Products = pd.DataFrame(products)
    os.makedirs(output_dir, exist_ok=True)
    Products.to_csv(f"{output_dir}/Products.csv", index=False)

def material_Gen(output_dir="Org1/PR_DB/DimTab"):
    materials = [
        {"Material_Id": 1, "Material_Name": "Cotton"},
        {"Material_Id": 2, "Material_Name": "Leather"},
        {"Material_Id": 3, "Material_Name": "Polyester"},
        {"Material_Id": 4, "Material_Name": "Denim"},
    ]
    Material = pd.DataFrame(materials)
    os.makedirs(output_dir, exist_ok=True)
    Material.to_csv(f"{output_dir}/Material.csv", index=False)

def date_Gen(output_dir="Org1/PR_DB/DimTab"):
    # Generate date range
    dates = pd.date_range(start= START_DATE, end="2024-12-31", freq="D")
    df = pd.DataFrame({
        "Day": dates.day,
        "Month": dates.month,
        "Year": dates.year,
        "Date_Id": range(1, len(dates) + 1)
    })
    os.makedirs(output_dir, exist_ok=True)
    df.to_csv(f"{output_dir}/Date.csv", index=False)

# Generate Sales Fact Table with Key Values
def sale_Gen():
    global SALE_COUNTER
    SALE_COUNTER = 0

    # Load existing tables
    products = pd.read_csv("Org1/PR_DB/DimTab/Products.csv")
    materials = pd.read_csv("Org1/PR_DB/DimTab/Material.csv")
    dates = pd.read_csv("Org1/PR_DB/DimTab/Date.csv")

    # Randomly sample 500 rows
    num_rows = 500
    np.random.seed(1) # For reproducibility

    product_ids = np.random.choice(products["Product_Id"], num_rows)
    material_ids = np.random.choice(materials["Material_Id"], num_rows)
    date_ids = np.random.choice(dates["Date_Id"], num_rows)

    # Generate random emissions (between 1 and 100, 2 decimals)
    emissions = np.random.uniform(1, 100, num_rows).round(2)

    # Add timestamp column
    # ISO format: YYYY-MM-DD HH:MM:SS.mmmmmm
    # ts = datetime.now().isoformat()

    # YYYY-MM-DD HH:MM:SS.sss
    # ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    
    ts = int(datetime.now().timestamp() * 1000)


    sales = pd.DataFrame({
        "Product_Id": product_ids,
        "Material_Id": material_ids,
        "Date_Id": date_ids,
        "Total_Emissions": emissions,
        "TS": ts
    })

    output_dir="Org1/PR_DB"
    sales.to_csv(f"{output_dir}/Sale_PR.csv", index=False)

    print("\nFile generated using StarSchemeGenerator and saved as 'Sale_PR.csv' in Org1/PR_DB folder.")

def sale_update():
    selected_file = "Org1/PR_DB/Sale_PR.csv"

    global SALE_COUNTER
    SALE_COUNTER += 1

    # Load existing tables
    products = pd.read_csv("Org1/PR_DB/DimTab/Products.csv")
    materials = pd.read_csv("Org1/PR_DB/DimTab/Material.csv")
    dates = pd.read_csv("Org1/PR_DB/DimTab/Date.csv")

    # Randomly sample 100 rows
    num_rows = 100
    np.random.seed(SALE_COUNTER) # For reproducibility

    product_ids = np.random.choice(products["Product_Id"], num_rows)
    material_ids = np.random.choice(materials["Material_Id"], num_rows)
    date_ids = np.random.choice(dates["Date_Id"], num_rows)

    # Generate random emissions (between 1 and 100, 2 decimals)
    emissions = np.random.uniform(1, 100, num_rows).round(2)

    # Add timestamp column
    ts = int(datetime.now().timestamp() * 1000)
    
    new_sales = pd.DataFrame({
        "Product_Id": product_ids,
        "Material_Id": material_ids,
        "Date_Id": date_ids,
        "Total_Emissions": emissions,
        "TS": ts
    })

    # Append new rows directly to the selected file
    new_sales.to_csv(selected_file, mode='a', header=False, index=False)

    print(f"\n'{selected_file}' was updated with new rows.")


def main():
    product_Gen()
    material_Gen()
    date_Gen()
    sale_Gen()
    

if __name__ == "__main__":
    main()