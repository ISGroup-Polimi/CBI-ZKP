import json
import os

dfm_path = os.path.join("Shared", "DFM_Sale.json")
with open(dfm_path, "r") as f:
    dfm = json.load(f)

def select_operations():
    operations = ["Rollup", "Dicing", "Slicing"]
    result = {}
    while True:
        print("Select the operation you want to perform:")
        for idx, op in enumerate(operations, 1):
            print(f"{idx}. {op}")
        print("0] Exit")
        # 1] Rollup
        # 2] Dicing
        # 3] Slicing
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

                elif op_name == "Dicing":
                    print("To do")

                elif op_name == "Slicing":
                    print("To do")
            else:
                print("Invalid selection.")
        except ValueError:
            print("Please enter a valid number.")
    print("Final operations result:", result)
    return result


def CLI_rollup():
    dim_hierarchy = dfm["dim_hierarchy"]

    print("Select one or more hierarchies to roll-up (comma separated numbers, e.g. 1,3):")
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
                print(f"Select a dimension to roll-up on for '{selected_hier}':")
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
