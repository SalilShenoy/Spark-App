import pandas as pd
from fastparquet import write

df = pd.DataFrame(None, columns = ['Accession Id', 
	'Starting Position', 'Allele', 'Individual', 'Dosage'])

df.to_csv('schema.csv')

write('schema.parq', df, compression='GZIP')