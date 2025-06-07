# This class is used to remove dimension for rollup operations
from ..models.olap_operations import OLAPOperation
from torch import nn
import torch

class RollUpModel(OLAPOperation):
    def __init__(self, remove_columns=None):
        super(RollUpModel, self).__init__()
        # ensure to sort the list of columns (1, 2, 3, ...) if present else create an empty list
        self.remove_columns = sorted(remove_columns) if remove_columns is not None else []
    
    def forward(self, x):
        if not self.remove_columns:
            return x  # Nessuna colonna da rimuovere, ritorna x inalterato
        
        # Crea una lista di intervalli per mantenere le colonne
        ranges_to_keep = []
        start = 0
        for col in self.remove_columns:
            if col > start:
                ranges_to_keep.append((start, col))
            start = col + 1
        
        # Aggiungi l'ultimo intervallo se c'è ancora qualcosa dopo l'ultima colonna rimossa 
        # es. [1, ..., 5] e remove_columns = [2, 3] -> append [4, 5]
        if start < x.size(1):
            ranges_to_keep.append((start, x.size(1)))
        
        # x_kept_parts lista di tensori x[:, s:e]
        # x[:, s:e] prende tutte le righe ":" e le colonne da "s" a "e"
        x_kept_parts = [x[:, s:e] for s, e in ranges_to_keep]
        # Concatena x_kept_parts lungo la dimensione delle colonne (dim=1)
        return torch.cat(x_kept_parts, dim=1)
