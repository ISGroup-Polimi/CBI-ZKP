# This program allows the user to select OLAP operations (Rollup, Slicing/Dicing)
import json
import os
import pandas as pd

dfm_path = os.path.join("Shared", "DFM_Sale.json")
with open(dfm_path, "r") as f:
    dfm_json = json.load(f)

def select_operations():
    operations = ["Rollup", "Slicing/Dicing"]
    result = {}
    selected_ops = set() # To allow only one instance of each operation
    while True:
        print("\nSelect the operation you want to perform:")
        for idx, op in enumerate(operations, 1):
            if op in selected_ops:
                print(f"{idx}] {op} (already selected)")
            else:
                print(f"{idx}] {op}")
        print("0] Exit")
        # 1] Rollup
        # 2] Slicing/Dicing
        # 0] Exit

        choice = input("Enter the number of the operation: ")
        if choice == "0":
            print("Exiting.")
            break
        try:
            choice_idx = int(choice) - 1
            if 0 <= choice_idx < len(operations):
                op_name = operations[choice_idx]
                if op_name in selected_ops:
                    print(f"{op_name} has already been selected. Please choose another operation.")
                    continue
                print(f"You selected: {op_name}")

                if op_name == "Rollup":
                    rollup_result = CLI_rollup()
                    if rollup_result:
                        result.setdefault("Rollup", []).extend(rollup_result["Rollup"])
                        selected_ops.add("Rollup")

                elif op_name == "Slicing/Dicing":
                    slice_result = CLI_slice_and_dice()
                    if slice_result:
                        result.setdefault("Dicing", []).extend(slice_result["Dicing"])
                        selected_ops.add("Dicing")

            else:
                print("Invalid selection.")
        except ValueError:
            print("Please enter a valid number.")
    print("Final operations result:", result)
    return result


def CLI_rollup():
    dim_hierarchy = dfm_json["dim_hierarchy"]

    print("\nSelect one or more hierarchies to roll-up (comma separated numbers, e.g. 1,3):")
    hierarchies = list(dim_hierarchy.keys())
    for i, dim in enumerate(hierarchies, 1):
        print(f"{i}] {dim}")
    # 1] Clothes Type
    # 2] Date
    # 3] Material

    hier_choices = input("Enter the numbers of the hierarchies: ")
    try:
        hier_indices = [int(idx.strip()) - 1 for idx in hier_choices.split(",") if idx.strip().isdigit()]
        selected_hiers = []
        for hier_idx in hier_indices:
            if 0 <= hier_idx < len(hierarchies):
                selected_hier = hierarchies[hier_idx]
                dimensions = dim_hierarchy[selected_hier]
                print(f"\nSelect a dimension to roll-up on for '{selected_hier}':")
                for j, dim in enumerate(dimensions, 1):
                    print(f"{j}] {dim}")
                # Clothes Type        Date         Material
                # 1] Category         1] Year      1] Material
                # 2] Product Name     2] Month
                #                     3] Day

                dim_choice = input("Enter the number of the dimension: ")
                try:
                    dim_idx = int(dim_choice) - 1
                    if 0 <= dim_idx < len(dimensions):
                        selected_dim = dimensions[dim_idx]
                        selected_hiers.append([selected_hier, selected_dim])
                    else:
                        print("Invalid dimension selection.")
                        return None
                except ValueError:
                    print("Please enter a valid number for dimension.")
                    return None
            else:
                print("Invalid hierarchy selection.")
                return None
        print(f"You selected to roll-up on: {selected_hiers}")
        return {"Rollup": selected_hiers}
    except ValueError:
        print("Please enter valid numbers for hierarchies.")
        return None

def CLI_slice_and_dice():
    dim_indexes = dfm_json["dim_indexes"]
    dimensions = list(dim_indexes.keys())

    print("\nSelect one or more dimensions to slice or dice (comma separated numbers, e.g. 1,3):")
    for i, dim in enumerate(dimensions, 1):
        print(f"{i}] {dim}")
    # Dimensions:
    # 1] Product Name
    # 2] Category
    # 3] Material
    # 4] Year
    # 5] Month
    # 6] Day

    dim_choices = input("Enter the numbers of the dimensions: ")
    try:
        dim_indices = [int(idx.strip()) - 1 for idx in dim_choices.split(",") if idx.strip().isdigit()]
        slice_dice_dict = {}

        for dim_idx in dim_indices:
            if 0 <= dim_idx < len(dimensions):
                dim_name = dimensions[dim_idx]
                dim_idx = dim_indexes[dim_name]
                print(f"\nYou selected to slice or dice on: {dim_name}")

                # If dimension is a Date: "Year", "Month", or "Day"
                if dim_name in ["Year", "Month", "Day"]:
                    value_input = input(f"Enter the {dim_name} value to filter (e.g., 2 for February): ")
                    try:
                        value = int(value_input.strip())
                        # Use filter_data to get the list of date_ids
                        date_ids = filter_data(value, dim_name) # implemented below
                        slice_dice_dict[dim_idx] = date_ids
                    except ValueError:
                        print("Invalid input for value.")
                        return None
                    continue  # Skip the rest of the loop for this dimension

                # For all other dimensions: "Product Name", "Category", "Material"
                else:
                    possible_values = dfm_json.get(dim_name)
                    if possible_values:
                        # For dicts like "Product Name": name -> id, build a list of names
                        if isinstance(possible_values, dict):
                            value_names = list(possible_values.keys())
                            print(f"Possible values for '{dim_name}':")
                            for i, val in enumerate(value_names):
                                print(f"{i}] {val}")
                            values_input = input(f"\nEnter the indices of the values to keep for '{dim_name}' (comma separated): ")
                            try:
                                indices = [int(v.strip()) for v in values_input.split(",") if v.strip().isdigit()]
                                # Map indices to names, then to IDs
                                values = [possible_values[value_names[i]] for i in indices if 0 <= i < len(value_names)]
                            except ValueError:
                                print("Invalid input for indices.")
                                return None
                        else:
                            # If possible_values is a list, keep your original logic
                            print(f"Possible values for '{dim_name}':")
                            for i, val in enumerate(possible_values):
                                print(f"{i}] {val}")
                            values_input = input(f"Enter the indices of the values to keep for '{dim_name}' (comma separated): ")
                            try:
                                values = [possible_values[int(v.strip())] for v in values_input.split(",") if v.strip().isdigit()]
                            except ValueError:
                                print("Invalid input for indices.")
                                return None
                    else:
                        values_input = input(f"Enter the values to keep for '{dim_name}' (comma separated): ")
                        values = []
                        for v in values_input.split(","):
                            v = v.strip()
                            if v.isdigit():
                                values.append(int(v))
                            else:
                                values.append(v)

                    # If only one value, store as int, else as list
                    if len(values) == 1:
                        slice_dice_dict[dim_idx] = values[0]
                    else:
                        slice_dice_dict[dim_idx] = values
            else:
                print("Invalid dimension selection.")
                return None

        print(f"You selected dicing: {slice_dice_dict}")
        return {"Dicing": [slice_dice_dict]}
    except ValueError:
        print("Please enter valid numbers for dimensions.")
        return None
    
# Returns a list of date_ids where the given type (Year, Month, or Day) equals number.
# Example: number=2, type="Month" returns all date_ids for February.
def filter_data(number, type):
    date_df = pd.read_csv("Org1/PR_DB/DimTab/Date.csv")

    # Normalize type input
    type = type.capitalize()

    if type not in ["Year", "Month", "Day"]:
        raise ValueError("type must be 'Year', 'Month', or 'Day'")
    
    # Filter rows where the column matches the number
    filtered = date_df[date_df[type] == number]
    
    # Return the list of Date_Id as integers
    return filtered["Date_Id"].tolist()