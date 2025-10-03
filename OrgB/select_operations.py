import json
import os

dfm_path = os.path.join("Shared", "DFM_Sale.json")
with open(dfm_path, "r") as f:
    dfm = json.load(f)

def select_operations():
    operations = ["Rollup/Dicing", "Slicing"]
    result = {}
    while True:
        print("\nSelect the operation you want to perform:")
        for idx, op in enumerate(operations, 1):
            print(f"{idx}] {op}")
        print("0] Exit")
        # 1] Rollup/Dicing
        # 2] Slicing
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

                if op_name == "Rollup/Dicing":
                    rollup_result = CLI_rollup()
                    if rollup_result:
                        result.setdefault("Rollup", []).extend(rollup_result["Rollup"])

                elif op_name == "Slicing":
                    slice_result = CLI_slice()
                    if slice_result:
                        result.setdefault("Slicing", []).extend(slice_result["Slicing"])

            else:
                print("Invalid selection.")
        except ValueError:
            print("Please enter a valid number.")
    print("Final operations result:", result)
    return result


def CLI_rollup():
    dim_hierarchy = dfm["dim_hierarchy"]

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
    
def CLI_slice():
    dim_index = dfm["dim_index"]
    dimensions = list(dim_index.keys())

    print("\nSelect one or more dimensions to slice (comma separated numbers, e.g. 1,3):")
    for i, dim in enumerate(dimensions, 1):
        print(f"{i}] {dim}")
    # 1] Product Name
    # 2] Category
    # 3] Material
    # 4] Year
    # 5] Month
    # 6] Day
    # 7] Total Emissions (kgCO2e)

    dim_choices = input("Enter the numbers of the dimensions: ")
    try:
        dim_indices = [int(idx.strip()) - 1 for idx in dim_choices.split(",") if idx.strip().isdigit()]
        selected_dims = []
        slicing_dict = {}

        for dim_idx in dim_indices:
            if 0 <= dim_idx < len(dimensions):
                dim_name = dimensions[dim_idx]
                col_idx = dim_index[dim_name]
                print(f"\nYou selected to slice on: {dim_name}")

                # Ask user for values to keep for this dimension
                values_input = input(f"Enter the values to keep for '{dim_name}' (comma separated): ")
                # Try to convert to int if possible, else keep as string
                values = []
                for v in values_input.split(","):
                    v = v.strip()
                    if v.isdigit():
                        values.append(int(v))
                    else:
                        # Try to map string to index if possible (e.g., for categories/materials/products)
                        # Otherwise, just keep as string
                        values.append(v)
                slicing_dict[col_idx] = values
            else:
                print("Invalid dimension selection.")
                return None

        print(f"You selected dicing: {slicing_dict}")
        return {"Slicing": [slicing_dict]}
    except ValueError:
        print("Please enter valid numbers for dimensions.")
        return None
    

