import pandas as pd
import glob
#integrazione di tutti i file csv
all_files = glob.glob("incidenti*.csv") 

l = []

for filename in all_files:
    file = pd.read_csv(filename, index_col=None, header=0, encoding='latin1', delimiter=';') 
    l.append(file)

combined = pd.concat(l, axis=0, ignore_index=True) 

combined.to_csv('combined_dataset.csv', index=False, sep=';')