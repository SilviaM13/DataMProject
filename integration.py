import pandas as pd
import glob

all_files = glob.glob("incidenti*.csv") #trova tutti i files con questo nome

l = []

for filename in all_files:
    file = pd.read_csv(filename, index_col=None, header=0, encoding='latin1', delimiter=';') #header per indicare che la prima riga è di intestazione e index_col=none per non usare nessuna colonna come indice
    l.append(file)

combined = pd.concat(l, axis=0, ignore_index=True) #axis=0 per concatenare per righe, l'altro indica che i nuovi dataframe concatenati avranno un nuovo indice

combined.to_csv('combined_dataset.csv', index=False, sep=';')