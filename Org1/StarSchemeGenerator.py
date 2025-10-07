# Star Schema Generator 
# - Generate Dimension Tables: Products, Material, Date
# - Generate Fact Table: Sale with foreign keys to dimension tables
# - Update Fact Table with new sales records
# - Update Dimension Tables

import json
import pandas as pd
import os
import numpy as np
from datetime import datetime

with open("Shared/DFM_Sale.json", "r") as f:
    dfm_json = json.load(f)
START_DATE = dfm_json["START_DATE"]
END_DATE = dfm_json["END_DATE"]

os.makedirs("Org1/PR_DB/DimTab", exist_ok=True)

SALE_COUNTER = 0

def product_Gen(output_dir="Org1/PR_DB/DimTab"):
    # Load product info from DFM_Sale.json
    product_names = dfm_json["Product Name"]  # Dict: {Product_Name: Product_Id}
    category_range = dfm_json["Category_range"]  # Dict: {Category: [start_id, end_id]}

    # Build Product_Id -> Category mapping
    id_to_category = {}
    for category, (start_id, end_id) in category_range.items():
        for pid in range(start_id, end_id + 1):
            id_to_category[pid] = category

    # Build products list
    products = []
    for pname, pid in product_names.items():  
        pid = int(pid)
        category = id_to_category.get(pid, "Unknown")
        products.append({
            "Product_Id": pid,
            "Product_Name": pname,
            "Category": category
        })

    Products = pd.DataFrame(products)
    os.makedirs(output_dir, exist_ok=True)
    Products.to_csv(f"{output_dir}/Products.csv", index=False)

def material_Gen(output_dir="Org1/PR_DB/DimTab"):
    # Load material info from DFM_Sale.json
    material_dict = dfm_json["Material"]  # Dict: {Material_Name: Material_Id}
    materials = []
    for mname, mid in material_dict.items():  
        mid = int(mid)
        materials.append({
            "Material_Id": mid,
            "Material_Name": mname
        })
    Material = pd.DataFrame(materials)
    os.makedirs(output_dir, exist_ok=True)
    Material.to_csv(f"{output_dir}/Material.csv", index=False)

def date_Gen(output_dir="Org1/PR_DB/DimTab"):
    # Generate date range
    dates = pd.date_range(start= START_DATE, end=END_DATE, freq="D")
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

    # ts = int(datetime.now().timestamp() * 1000)
    ts = int(datetime(2025, 1, 1).timestamp() * 1000)


    sales = pd.DataFrame({
        "Product_Id": product_ids,
        "Material_Id": material_ids,
        "Date_Id": date_ids,
        "Total_Emissions": emissions,
        "TS": ts
    })

    sales.to_csv("Org1/PR_DB/Sale_PR.csv", index=False)

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

# Update Products dimension table and DFM_Sale.json with new products and categories
def update_products():
    products_file = "Org1/PR_DB/DimTab/Products.csv"
    products = pd.read_csv(products_file)

    new_products = [
        {"Product_Id": 13, "Product_Name": "Baseball Cap", "Category": "Hat"},
        {"Product_Id": 14, "Product_Name": "Beanie", "Category": "Hat"},
        {"Product_Id": 15, "Product_Name": "Cowboy Hat", "Category": "Hat"},
        {"Product_Id": 16, "Product_Name": "Bucket Hat", "Category": "Hat"},
    ]
    new_products_df = pd.DataFrame(new_products)

    updated_products = pd.concat([products, new_products_df], ignore_index=True)
    updated_products.to_csv(products_file, index=False)
    print(f"\n'{products_file}' was updated with new products.")

    # --------------------------------------------------Update DFM_Sale.json
    with open("Shared/DFM_Sale.json", "r") as f:
        dfm_json = json.load(f)

    # Update Product Name mapping
    product_name_dict = dfm_json["Product Name"]
    for prod in new_products:
        product_name_dict[prod["Product_Name"]] = prod["Product_Id"]

    # Update Category_range if needed
    category_range = dfm_json.get("Category_range", {})
    for prod in new_products:
        cat = prod["Category"]
        pid = prod["Product_Id"]
        if cat not in category_range:
            category_range[cat] = [pid, pid]
        else:
            # Expand the range if needed
            category_range[cat][0] = min(category_range[cat][0], pid)
            category_range[cat][1] = max(category_range[cat][1], pid)
    dfm_json["Category_range"] = category_range

    # Optionally update Category section
    if "Category" in dfm_json and "Hat" not in dfm_json["Category"]:
        # Assign a new category ID for "Hat"
        max_cat_id = max(dfm_json["Category"].values())
        dfm_json["Category"]["Hat"] = max_cat_id + 1

    with open("Shared/DFM_Sale.json", "w") as f:
        json.dump(dfm_json, f, indent=4)
    print("\n'Shared/DFM_Sale.json' was updated with new products and categories.")


def main():
    product_Gen()
    material_Gen()
    date_Gen()
    sale_Gen()
    

if __name__ == "__main__":
    main()