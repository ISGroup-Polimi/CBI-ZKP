import torch
import torch.nn as nn

class OLAPWithHash(nn.Module):
    def __init__(self, base_model):
        super().__init__()
        self.base_model = base_model

    def forward(self, x, data_hash):
        y = self.base_model(x)
        return y, data_hash