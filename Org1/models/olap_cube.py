import torch
from sklearn.preprocessing import LabelEncoder # to convert categorical string data into numeric labels
import json

from Shared.Dim_ID_Converter import create_mappings_json

class OLAPCube:
    def __init__(self, df): # The constructor receives as input a >..
        self.df = df                                #  ..< pandas DataFrame (df) 
        self.category_mappings = create_mappings_json()
        # Save the mapping to Shared/map.json
        with open("Shared/map.json", "w") as f:
            json.dump(self.category_mappings, f, indent=2)
        # Map categorical columns to integer codes
        for col, mapping in self.category_mappings.items():
            if col in self.df.columns:
                self.df[col] = self.df[col].map(lambda x: mapping.get(str(x), x))

    # This method is used to convert the values of the DataFrame to a torch tensor of type float32
    def to_tensor(self):    
        return torch.tensor(self.df.values, dtype=torch.float32)

    # This method applies a specified operation (model) to the tensor data
    def execute_model(self, model, tensor_data):
        return model(tensor_data) # slicing/ roll_op/ dicing_model
    # In PyTorch, any class that inherits from nn.Module can be called like a function (i.e., model(tensor_data)), which internally calls its forward() method.
    # So, model(tensor_data) is equivalent to model.forward(tensor_data)