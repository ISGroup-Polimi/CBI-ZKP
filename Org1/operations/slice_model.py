# Example: SlicingModel({14: 1,   21: 12,   27: 0})
# -> filter to have only the rows where column 14 is ==1, column 21 is ==12 and column 27 is ==0

import torch
from ..models.olap_operations import OLAPOperation
from torch import nn

class SliceModel(OLAPOperation):
    def __init__(self, filter_conditions):
        super(SliceModel, self).__init__()
        self.filter_conditions = filter_conditions

    def forward(self, x):
        # create a mask of ones with the same size as the rows of x (x.size(0))
        mask = torch.ones(x.size(0), dtype=torch.bool)
        for column, value in self.filter_conditions.items():
            mask = mask & (x[:, column] == value) # x[:, column] -> select all rows of the "column"
            # mask = tensor[T, F, T, ...] 
        return x * mask.unsqueeze(1) 
        # mask.unsqueeze(1) = tensor([[T],    in this way doing *x we make zero all the rows that do not match the conditions
        #                             [F],
        #                             [T], 
        #                             ...])
        # make zero the rows that do not match the conditions (do not eliminate them)
        # the model always works on tensors of fixed shape, so we need to return a tensor with rows of 0
