import pandas as pd
import json
import os

START_DATE = "2020-01-01"


## DATE CONVERTER

# Return date (datetime.date) from Date_Id
def get_date_from_id(date_id):

    date = pd.to_datetime(START_DATE) + pd.Timedelta(days=date_id - 1)

    return date.date()

# Return Date_Id from date ('YYYY-MM-DD'string or datetime.date)
def get_id_from_date(date):

    date = pd.to_datetime(date)
    start = pd.to_datetime(START_DATE)

    date_id = (date - start).days + 1
    
    if date_id < 1:
        raise ValueError("Couldn't get Date_Id: date is before START_DATE")
    return date_id


## MATERIAL CONVERTER
with open("Shared/DFM_Sale.json", "r") as f:
    data = json.load(f)

materials = data["materials"]

# Return the list of material names from a list of material IDs
def get_materials_from_ids(material_ids):
    # Create a reverse dictionary to map IDs to names
    id_to_name = {v: k for k, v in materials.items()}
    result = []
    for mid in material_ids:
        if mid not in id_to_name:
            raise ValueError(f"Material ID '{mid}' not found in materials.")
        result.append(id_to_name[mid])
    return result

# Return the list of material IDs from a list of material names
def get_ids_from_materials(material_names):
    ids = []
    for name in material_names:
        found = False
        for mid, mat_name in materials.items():
            if mat_name == name:
                ids.append(int(mid))
                found = True
                break
        if not found:
            raise ValueError(f"Material name '{name}' not found in materials.")
    return ids

## PRODUCT CONVERTER
with open("Shared/DFM_Sale.json", "r") as f:
    data = json.load(f) 

products = data["products"]
# {'Running Shoes': 1, 'Leather Boots': 2, ..., 'Tank Top': 12}


# Return the list of product names from a list of product IDs
def get_products_from_ids(product_ids):
    # Create a reverse dictionary to map IDs to names
    id_to_name = {v: k for k, v in products.items()}
    result = []
    for pid in product_ids:
        if pid not in id_to_name:
            raise ValueError(f"Product ID '{pid}' not found in products.")
        result.append(id_to_name[pid])
    return result

# Return the list of product IDs from a list of product names
def get_ids_from_products(product_names):
    ids = []
    for name in product_names:
        found = False
        for pid, prod_name in products.items():
            if prod_name == name:
                ids.append(int(pid))
                found = True
                break
        if not found:
            raise ValueError(f"Product name '{name}' not found in products.")
    return ids

#CATEGORY
with open("Shared/DFM_Sale.json", "r") as f:
    data = json.load(f) 

categories = data["categories"]

def get_categories_from_ids(category_ids):
    # Create a reverse dictionary to map IDs to names
    id_to_name = {v: k for k, v in categories.items()}
    result = []
    for cid in category_ids:
        if cid not in id_to_name:
            raise ValueError(f"Category ID '{cid}' not found in categories.")
        result.append(id_to_name[cid])
    return result

def get_ids_from_categories(category_names):
    ids = []
    for name in category_names:
        found = False
        for cid, cat_name in categories.items():
            if cat_name == name:
                ids.append(int(cid))
                found = True
                break
        if not found:
            raise ValueError(f"Category name '{name}' not found in categories.")
    return ids

# Convert a CSV with Dim IDs to a human-readable CSV, saving it in the "test" folder
def CSV_converter(path):
    df = pd.read_csv(path)

    csv_filename = os.path.basename(path)
    print(f"Converting file: {csv_filename}")

    product_names = get_products_from_ids(df["Product_Id"])
    categories = get_categories_from_ids(df["Product_Id"])
    materials = get_materials_from_ids(df["Material_Id"])
    dates = [get_date_from_id(date_id) for date_id in df["Date_Id"]]

    years = [d.year for d in dates]
    months = [d.month for d in dates]
    days = [d.day for d in dates]

    out_df = pd.DataFrame({
        "Product Name": product_names,
        "Category": categories,
        "Material": materials,
        "Year": years,
        "Month": months,
        "Day": days,
        "Total_Emissions": df["Total_Emissions"]
    })

    # filename + "_Converted.csv"
    output_path = os.path.join("test", f"{os.path.splitext(csv_filename)[0]}_Converted.csv")
    out_df.to_csv(output_path, index=False)
    print(f"Converted CSV saved as '{output_path}'")