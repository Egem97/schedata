import warnings
import pandas as pd

def setup_pandas_warnings():
    warnings.simplefilter(action='ignore', category=FutureWarning)
    pd.options.mode.chained_assignment = None