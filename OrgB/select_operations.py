import json
import os

dfm_path = os.path.join("Shared", "DFM_Sale.json")
with open(dfm_path, "r") as f:
    dfm = json.load(f)

def select_operations():
    operations = ["Rollup", "Dicing", "Slicing"]
    while True:
        print("Select the operation you want to perform:")
        for idx, op in enumerate(operations, 1):
            print(f"{idx}. {op}")
        print("0. Exit")
        choice = input("Enter the number of the operation: ")
        if choice == "0":
            print("Exiting.")
            return None
        try:
            choice_idx = int(choice) - 1
            if 0 <= choice_idx < len(operations):
                print(f"You selected: {operations[choice_idx]}")
                if operations[choice_idx] == "Rollup":
                    CLI_rollup()
                else:
                    return operations[choice_idx]
            else:
                print("Invalid selection.")
        except ValueError:
            print("Please enter a valid number.")


def CLI_rollup():
    dim_hierarchy = dfm["dim_hierarchy"]

    print("Select a hierarchy to roll-up:")
    hierarchies = list(dim_hierarchy.keys())
    for i, dim in enumerate(hierarchies, 1):
        print(f"{i}] {dim}")
    # 1] Clothes Type
    # 2] Date
    # 3] Material

    hier_choice = input("Enter the number of the hierarchy: ")
    try:
        hier_idx = int(hier_choice) - 1
        if 0 <= hier_idx < len(hierarchies):
            selected_hier = hierarchies[hier_idx]

            # Now select the dimension
            dimensions = dim_hierarchy[selected_hier]
            print("Select a dimension to roll-up on:")
            for i, dim in enumerate(dimensions, 1):
                print(f"{i}] {dim}")
            # ex. Date
            # 1] Year
            # 2] Month
            # 3] Day
            
            dim_choice = input("Enter the number of the dimension: ")
            try:
                dim_idx = int(dim_choice) - 1
                if 0 <= dim_idx < len(dimensions):
                    selected_dim = dimensions[dim_idx]
                    print(f"You selected to roll-up on dimension: {selected_dim}")
                    return ("Rollup", selected_hier, selected_dim)
                else:
                    print("Invalid dimension selection.")
            except ValueError:
                print("Please enter a valid number for dimension.")
        else:
            print("Invalid hierarchy selection.")
    except ValueError:
        print("Please enter a valid number for hierarchy.")
