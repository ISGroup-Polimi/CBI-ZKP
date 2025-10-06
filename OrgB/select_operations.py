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
    while True:
        print("\nSelect the operation you want to perform:")
        for idx, op in enumerate(operations, 1):
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
                print(f"You selected: {op_name}")

                if op_name == "Rollup":
                    rollup_result = CLI_rollup()
                    if rollup_result:
                        result.setdefault("Rollup", []).extend(rollup_result["Rollup"])

                elif op_name == "Slicing/Dicing":
                    slice_result = CLI_slice_and_dice()
                    if slice_result:
                        result.setdefault("Dicing", []).extend(slice_result["Dicing"])

            else:
                print("Invalid selection.")
        except ValueError:
            print("Please enter a valid number.")
            
    print("Final operations result:", result)

    condensed = condense_operation(result)
    print("Final operations result:", condensed)

    return condensed


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
                dim_col_idx = dim_indexes[dim_name]
                print(f"\nYou selected to slice or dice on: {dim_name}")

                # If dimension is a Date: "Year", "Month", or "Day"
                if dim_name == "Year":
                    values = CLI_Year()
                    if values is None:
                        print("No values selected.")
                        return None
                    if len(values) == 1:
                        slice_dice_dict[dim_col_idx] = values[0]
                    else:
                        slice_dice_dict[dim_col_idx] = values
                    continue  # Skip the rest of the loop for this dimension

                elif dim_name == "Month":
                    values = CLI_Month()
                    if values is None:
                        print("No values selected.")
                        return None
                    if len(values) == 1:
                        slice_dice_dict[dim_col_idx] = values[0]
                    else:
                        slice_dice_dict[dim_col_idx] = values
                    continue

                elif dim_name == "Day":
                    values = CLI_Day()
                    if values is None:
                        print("No values selected.")
                        return None
                    if len(values) == 1:
                        slice_dice_dict[dim_col_idx] = values[0]
                    else:
                        slice_dice_dict[dim_col_idx] = values
                    continue

                # For all other dimensions: "Product Name", "Category", "Material"
                else:
                    possible_values = dfm_json.get(dim_name)
                    if possible_values:
                        if isinstance(possible_values, dict):
                            value_names = list(possible_values.keys())
                            print(f"Possible values for '{dim_name}':")
                            for i, val in enumerate(value_names):
                                print(f"{i}] {val}")
                            values_input = input(f"\nEnter the indices of the values to keep for '{dim_name}' (comma separated): ")
                            try:
                                indices = [int(v.strip()) for v in values_input.split(",") if v.strip().isdigit()]
                                values = [possible_values[value_names[i]] for i in indices if 0 <= i < len(value_names)]
                            except ValueError:
                                print("Invalid input for indices.")
                                return None
                        else:
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

                    if len(values) == 1:
                        slice_dice_dict[dim_col_idx] = values[0]
                    else:
                        slice_dice_dict[dim_col_idx] = values
            else:
                print("Invalid dimension selection.")
                return None

        print(f"You selected dicing: {slice_dice_dict}")
        return {"Dicing": [slice_dice_dict]}
    except ValueError:
        print("Please enter valid numbers for dimensions.")
        return None
    
def CLI_Year():
    start_date = dfm_json["START_DATE"]
    end_date = dfm_json["END_DATE"]

    years = list(range(int(start_date[:4]), int(end_date[:4]) + 1))
    print("Available years:")
    for year in years:
        print(f"- {year}")
    years_input = input("Enter the years you want to select (comma separated, e.g. 2021,2022): ")
    selected_years = []
    for y in years_input.split(","):
        y = y.strip()
        if y.isdigit() and int(y) in years:
            selected_years.append(int(y))
    if not selected_years:
        print("No valid years selected.")
        return None
    return selected_years

def CLI_Month():
    print("Available months: 1 to 12")
    months_input = input("Enter the months you want to select (comma separated, e.g. 1,2 for Jan, Feb): ")
    selected_months = []
    for m in months_input.split(","):
        m = m.strip()
        if m.isdigit() and 1 <= int(m) <= 12:
            selected_months.append(int(m))
    if not selected_months:
        print("No valid months selected.")
        return None
    return selected_months

def CLI_Day():
    print("Available days: 1 to 31")
    days_input = input("Enter the days you want to select (comma separated, e.g. 1,15 for 1st and 15th): ")
    selected_days = []
    for d in days_input.split(","):
        d = d.strip()
        if d.isdigit() and 1 <= int(d) <= 31:
            selected_days.append(int(d))
    if not selected_days:
        print("No valid days selected.")
        return None
    return selected_days

# Condense operations to merge multiple same operations
# operations = {"Dicing": [{3: [2022]}], "Dicing": [{3: [2020]}]} into {"Dicing": [{3: [2022, 2020]}]}  
def condense_operation(result):
    condensed = {}
    for op in result:
        if op not in condensed:
            condensed[op] = []
        condensed[op].extend(result[op])

    # For Dicing and Rollup, merge dicts with same keys
    for op in ["Dicing", "Rollup"]:
        if op in condensed:
            merged = {}
            for d in condensed[op]:
                for k, v in d.items():
                    if k not in merged:
                        merged[k] = []
                    # Always treat v as a list for merging
                    if isinstance(v, list):
                        merged[k].extend(v)
                    else:
                        merged[k].append(v)
            # Remove duplicates and sort
            for k in merged:
                merged[k] = sorted(set(merged[k]))
            condensed[op] = [merged]
    return condensed