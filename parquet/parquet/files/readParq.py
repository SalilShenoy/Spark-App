import pandas as pd
import numpy as np
from fastparquet import ParquetFile

pf = ParquetFile('adult.parq')
df = pf.to_pandas()

df.to_csv('adult.csv')