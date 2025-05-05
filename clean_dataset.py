import pandas as pd
import re


file_name = "combined_dataset.csv"
file = pd.read_csv(file_name, index_col=None, header=0, delimiter=';', encoding='latin1', low_memory=False)


#rimozione colonne vuote non utili
file = file.drop(columns=["Unnamed: 37", "Longitude", "Latitude", "Deceduto", "NUM_RISERVATA", "STRADA2"], errors="ignore")

#ridenominazione delle colonne
file = file.rename(columns={
    "CinturaCascoUtilizzato": "Cintura/Casco",
    "NUM_FERITI": "Feriti",
    "NUM_ILLESI": "Illesi",
    "NUM_MORTI": "Morti",
    "Tipolesione": "TipoLesione",
    "STRADA1": "Strada",
    "Localizzazione1": "ClassificazioneStrada",
    "Localizzazione2": "Localizzazione",
    "Gruppo": "StazionePolizia",
    "particolaritastrade" : "ParticolaritaStrada",
    "DecedutoDopo": "DecedutoOspedale", 
})

#pulizia colonna "DecedutoOspedale" sostituendo "Non Decdeuto" con "No" e riempiendo le vuote con "No"
file["DecedutoOspedale"] = file["DecedutoOspedale"].replace({"NON DECEDUTO": "No",}).fillna("No")
# Se la cella contiene la parola "Deceduto", sostituiamo con "Si"
file["DecedutoOspedale"] = file["DecedutoOspedale"].apply(lambda x: "Si" if "DECEDUTO" in str(x) else x)
file.loc[file["TipoLesione"].isin(["Deceduto sul posto", "Deceduto durante prime cure"]), "DecedutoOspedale"] = "Si"


# Sostituire valori in "Tipolesione"
file["TipoLesione"] = file["TipoLesione"].replace({"Rimandato": "Ricovero", "Ricoverato": "Ricovero","Deceduto durante prime cure": "Deceduto",
                                                   "Deceduto durante trasporto": "Deceduto","Deceduto sul posto": "Deceduto"})

# Unire Strada02 e Chilometrica in una nuova colonna "Strada_Km"
#oppure chiamala posizione
file['Strada02'] = file['Strada02'] + ' ' + file['Chilometrica'].fillna('')
file.drop(columns=['Chilometrica'], inplace=True)

#sostituire "da specificare" con il valore di "DaSpecificare"**
file["Localizzazione"] = file.apply(lambda row: row["DaSpecificare"] if str(row["Localizzazione"]).strip().lower() == "da specificare" and pd.notna(row["DaSpecificare"]) 
else row["Localizzazione"], axis=1)
file.drop(columns=['DaSpecificare'], inplace=True)

file['Localizzazione'] = file['Localizzazione'] + ' ' + file['Strada02'].fillna('')
file.drop(columns=['Strada02'], inplace=True)
file['Localizzazione'] = file['Localizzazione'].replace('. ', 'Non rilevata')

#impostare "23:00:00" se manca l'orario nella colonna "DataOraIncidente"
file["DataOraIncidente"] = file["DataOraIncidente"].apply(lambda x: x + " 23:00:00" if re.match(r"^\d{2}/\d{2}/\d{4}$", str(x).strip()) else x)

file["Confermato"] = pd.to_numeric(file["Confermato"], errors="coerce").fillna(0).astype(int)
file["Confermato"] = file["Confermato"].replace(-1, 1)

fill_values = {
        "Localizzazione" : "Non rilevata",
        "ParticolaritaStrada": "Non rilevata",
        "TipoStrada" : "Non rilevata",
        "FondoStradale": "Non rilevato",
        "Pavimentazione": "Non rilevata",
        "Segnaletica": "Non rilevata",
        "CondizioneAtmosferica": "Non rilevata",
        "Traffico" : "Non rilevato",
        "Visibilita" : "Non rilevata",
        "Illuminazione" : "Non rilevata",
        "Longitudine" : "Non rilevata",
        "Latitudine" : "Non rilevata",
        "Feriti" : "Non rilevati",
        "Morti" : "Non rilevati",
        "Illesi" : "Non rilevati"
    }

file = file.fillna(value=fill_values)
file.loc[file["StatoVeicolo"] != "Sosta", "Airbag"] = file["Airbag"].fillna("Non accertato")
file.loc[file["StatoVeicolo"] != "Sosta", "Cintura/Casco"] = file["Cintura/Casco"].fillna("Non accertato")

#file.loc[file["TipoPersona"] == "Pedone", ["TipoVeicolo", "StatoVeicolo"]] = "Non rilevato"

#"Progressivo" in numeri interi, rimuovendo .0
file["Progressivo"] = pd.to_numeric(file["Progressivo"], errors="coerce").fillna(0).astype(int)


# ✅ Convertire la colonna in formato datetime (se non lo è già)
file["DataOraIncidente"] = pd.to_datetime(file["DataOraIncidente"], errors="coerce", format="%d/%m/%Y %H:%M:%S")
# ✅ Ordinare il dataset in base a "DataOraIncidente"
file = file.sort_values(by="DataOraIncidente", ascending=True)


# Trova il valore più frequente di "ClassificazioneStrada" per ogni "Strada"
most_frequent_classification = file.groupby("Strada")["ClassificazioneStrada"].agg(
    lambda x: x.mode()[0] if not x.mode().empty else x.iloc[0]  # Se non c'è una moda, prende il primo valore disponibile
)

# Riempire i valori mancanti in "ClassificazioneStrada" con il valore più comune della stessa "Strada"
def propagate_localization(group):
    if not group['ClassificazioneStrada'].mode().empty:
        common_value = group['ClassificazioneStrada'].mode()[0]  
    else:
        common_value = group['ClassificazioneStrada'].iloc[0]  
    group['ClassificazioneStrada'] = common_value
    return group

file = file.groupby('Strada', group_keys=False).apply(propagate_localization)



def propagate_particolarita(group):
    if not group['ParticolaritaStrada'].mode().empty:
        common_value = group['ParticolaritaStrada'].mode()[0]  
    else:
        common_value = group['ParticolaritaStrada'].iloc[0] 

    group['ParticolaritaStrada'] = common_value
    return group

#file = file.groupby(['Strada', 'Localizzazione'], group_keys=False).apply(propagate_particolarita)
file = file.groupby(['Strada', 'Localizzazione'], group_keys=False).apply(propagate_particolarita)

file.reset_index(drop=True, inplace=True)

def check_consistency(group, columns):
    inconsistencies = {}
    for col in columns:
        if group[col].nunique() > 1:  # Se ci sono più valori diversi per la stessa colonna
            inconsistencies[col] = group[col].unique()
    return inconsistencies

columns_to_check = [
    "StazionePolizia", "DataOraIncidente", "ClassificazioneStrada", "Strada", "Localizzazione",
    "NaturaIncidente", "ParticolaritaStrada", "TipoStrada", "FondoStradale",
    "Pavimentazione", "Segnaletica", "CondizioneAtmosferica", "Traffico",
    "Visibilita", "Illuminazione", "Feriti", "Morti", "Illesi", "Longitudine",
    "Latitudine", "Confermato"
]

inconsistent_rows = []

for protocollo, group in file.groupby("Protocollo"):
    inconsistencies = check_consistency(group, columns_to_check)
    if inconsistencies:
        inconsistent_rows.append((protocollo, inconsistencies))

#mi stampo i protocolli per capire dove sono presenti inconsistenze
for protocollo, inconsistencies in inconsistent_rows:
    print(f"⚠ Protocollo {protocollo} ha valori inconsistenti in: {inconsistencies}")



#salvataggio dataset pulito
cleaned_file = "cleaned_dataset.csv"
file.to_csv(cleaned_file, index=False, sep=';', encoding='latin1')

print(f"Dataset pulito salvato in: {cleaned_file}")
