import pandas as pd 
import numpy as np
from fastparquet import write

df = pd.read_csv('/etc/adult.data', names = ["Age", "Workclass", "fnlwgt", "Education", "Education_Num", "Martial_Status",
        "Occupation", "Relationship", "Race", "Sex", "Capital_Gain", "Capital_Loss",
        "Hours_per_week", "Country", "Target"])

write('adult.parq', df, compression='GZIP')
