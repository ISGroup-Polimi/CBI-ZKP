import pandas as pd
import os
import numpy as np

from Org1.Dim_ID_Converter import START_DATE # ="2020-01-01"

os.makedirs("Org1/PrivateDB/DimTab", exist_ok=True)

def Product_Gen(output_dir="Org1/PrivateDB/DimTab"):
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

def Material_Gen(output_dir="Org1/PrivateDB/DimTab"):
    materials = [
        {"Material_Id": 1, "Material_Name": "Cotton"},
        {"Material_Id": 2, "Material_Name": "Leather"},
        {"Material_Id": 3, "Material_Name": "Polyester"},
        {"Material_Id": 4, "Material_Name": "Denim"},
    ]
    Material = pd.DataFrame(materials)
    os.makedirs(output_dir, exist_ok=True)
    Material.to_csv(f"{output_dir}/Material.csv", index=False)

def Date_Gen(output_dir="Org1/PrivateDB/DimTab"):

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

def Sale_Gen(output_dir="Org1/PrivateDB"):

    # Load existing tables
    products = pd.read_csv("Org1/PrivateDB/DimTab/Products.csv")
    materials = pd.read_csv("Org1/PrivateDB/DimTab/Material.csv")
    dates = pd.read_csv("Org1/PrivateDB/DimTab/Date.csv")

    # Randomly sample 2000 rows
    num_rows = 2000
    np.random.seed(1) # For reproducibility

    product_ids = np.random.choice(products["Product_Id"], num_rows)
    material_ids = np.random.choice(materials["Material_Id"], num_rows)
    date_ids = np.random.choice(dates["Date_Id"], num_rows)

    # Generate random emissions (between 1 and 100, 2 decimals)
    emissions = np.random.uniform(1, 100, num_rows).round(2)

    sales = pd.DataFrame({
        "Product_Id": product_ids,
        "Material_Id": material_ids,
        "Date_Id": date_ids,
        "Total_Emissions": emissions
    })

    sales.to_csv(f"{output_dir}/Sale_Private.csv", index=False)

def main():
    Product_Gen()
    Material_Gen()
    Date_Gen()
    Sale_Gen()
    

if __name__ == "__main__":
    main()