import psycopg2
import pandas as pd


#connection
conn = psycopg2.connect(
    host="localhost",
    database= "DBproject",
    user= "postgres",
    password= "silvia"
)
cur = conn.cursor()

#creazione delle tabelle
# va messo dentro execute se serve
#DROP TABLE IF EXISTS coinvolti;
#DROP TABLE IF EXISTS incidenti;
#DROP TABLE IF EXISTS strade;

cur.execute("""
CREATE TABLE IF NOT EXISTS Incidenti (
    Protocollo VARCHAR(7) PRIMARY KEY,
    StazionePolizia INT,
    DataOraIncidente TIMESTAMP,
    Strada VARCHAR,
    NaturaIncidente VARCHAR,
    Feriti INT,
    Morti INT,
    Illesi INT,
    Longitudine VARCHAR,
    Latitudine VARCHAR,
    Confermato INT
);
            
CREATE TABLE IF NOT EXISTS Strade (
    Protocollo VARCHAR(7) PRIMARY KEY,
    ClassificazioneStrada VARCHAR,
    Strada VARCHAR,
    Localizzazione VARCHAR,
    ParticolaritaStrada VARCHAR,
    TipoStrada VARCHAR,
    FondoStradale VARCHAR,
    Pavimentazione VARCHAR,
    Segnaletica VARCHAR,
    CondizioneAtmosferica VARCHAR,
    Traffico VARCHAR,
    Visibilita VARCHAR,
    Illuminazione VARCHAR,
    FOREIGN KEY (Protocollo) REFERENCES Incidenti(Protocollo)        
);


CREATE TABLE IF NOT EXISTS Coinvolti (
    id SERIAL PRIMARY KEY,
    Protocollo VARCHAR,
    Progressivo INT,
    TipoVeicolo VARCHAR,
    StatoVeicolo VARCHAR,
    TipoPersona VARCHAR,
    Sesso CHAR(1),
    TipoLesione VARCHAR,
    DecedutoOspedale VARCHAR,
    CinturaCasco VARCHAR,
    Airbag VARCHAR,
    FOREIGN KEY (Protocollo) REFERENCES Incidenti(Protocollo)
);
""")

conn.commit()
print("Tabelle create con successo!")
#nella tabella coinvolti, la chiave deve essere composta, perchè per uno stesso incidente esistono più
#persone, mettendo così il protocollo può apparire più volte con lo stesso numero, purchè la persona sia diversa
# === LETTURA CSV ===
df = pd.read_csv("cleaned_dataset.csv", sep=";")

# === FUNZIONE PER GESTIRE VALORI NULLI ===
def safe_value(val):
    return None if pd.isna(val) else val


# === 1. INSERIMENTO INCIDENTI ===
incidenti_seen = set()  # per evitare duplicati
for _, row in df.iterrows():
    protocollo = row["Protocollo"]
    if protocollo in incidenti_seen:
        continue
    incidenti_seen.add(protocollo)

    cur.execute("""
        INSERT INTO Incidenti (
            Protocollo, StazionePolizia, DataOraIncidente,
            Strada, NaturaIncidente, Feriti, Morti, Illesi,
            Longitudine, Latitudine, Confermato
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (Protocollo) DO NOTHING
    """, (
        safe_value(row["Protocollo"]),
        safe_value(row["StazionePolizia"]),
        safe_value(row["DataOraIncidente"]),
        safe_value(row["Strada"]),
        safe_value(row["NaturaIncidente"]),
        safe_value(row["Feriti"]),
        safe_value(row["Morti"]),
        safe_value(row["Illesi"]),
        safe_value(row["Longitudine"]),
        safe_value(row["Latitudine"]),
        safe_value(row["Confermato"]),
    ))


# === 2. INSERIMENTO STRADE ===
strade_seen = set()
for _, row in df.iterrows():
    protocollo = row["Protocollo"]
    if protocollo in strade_seen:
        continue
    strade_seen.add(protocollo)

    cur.execute("""
        INSERT INTO Strade (
            Protocollo, ClassificazioneStrada, Strada, Localizzazione,
            ParticolaritaStrada, TipoStrada, FondoStradale, Pavimentazione,
            Segnaletica, CondizioneAtmosferica, Traffico, Visibilita, Illuminazione
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        safe_value(row["Protocollo"]),
        safe_value(row["ClassificazioneStrada"]),
        safe_value(row["Strada"]),
        safe_value(row["Localizzazione"]),
        safe_value(row["ParticolaritaStrada"]),
        safe_value(row["TipoStrada"]),
        safe_value(row["FondoStradale"]),
        safe_value(row["Pavimentazione"]),
        safe_value(row["Segnaletica"]),
        safe_value(row["CondizioneAtmosferica"]),
        safe_value(row["Traffico"]),
        safe_value(row["Visibilita"]),
        safe_value(row["Illuminazione"]),
    ))


# === 3. INSERIMENTO COINVOLTI ===
for _, row in df.iterrows():
    cur.execute("""
        INSERT INTO Coinvolti (
            Protocollo, Progressivo, TipoVeicolo, StatoVeicolo,
            TipoPersona, Sesso, TipoLesione, DecedutoOspedale,
            CinturaCasco, Airbag
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        safe_value(row["Protocollo"]),
        safe_value(row["Progressivo"]),
        safe_value(row["TipoVeicolo"]),
        safe_value(row["StatoVeicolo"]),
        safe_value(row["TipoPersona"]),
        safe_value(row["Sesso"]),
        safe_value(row["TipoLesione"]),
        safe_value(row["DecedutoOspedale"]),
        safe_value(row["Cintura/Casco"]),
        safe_value(row["Airbag"]),
    ))


# === COMMIT E CHIUSURA ===
conn.commit()
cur.close()
conn.close()

print("Dati inseriti con successo!")
