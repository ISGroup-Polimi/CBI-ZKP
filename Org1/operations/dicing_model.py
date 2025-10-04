# Dicing: select specific values for multiple dimensions (columns)
# This is also slice model
# Example conditions: {2: [0, 2]} means select rows where column index 2 has value 0 or 2
# {2: 3, 4: [0, 2]} allowed
# {2:0, 2:2} not allowed
from ..models.olap_operations import OLAPOperation
import torch
from torch import nn

class DicingModel(OLAPOperation):
    def __init__(self, conditions):
        super(DicingModel, self).__init__()
        self.conditions = conditions # ex. conditions = {2: [0, 2], 4: 3}
    
    def forward(self, x):
        mask = torch.ones(x.size(0), dtype=torch.bool, device=x.device)
        for column, value in self.conditions.items():
            # check if the value is a list (for multiple values in the same column)
            if isinstance(value, list):
                # Efficiently check if x[:, column] is in value
                mask = mask & torch.isin(x[:, column], torch.tensor(value, dtype=x.dtype, device=x.device))
            else:
                mask = mask & (x[:, column] == value)
        return x * mask.unsqueeze(1)
    