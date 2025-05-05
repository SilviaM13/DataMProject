from neo4j import GraphDatabase
import pandas as pd

# Connessione al DB
uri = "bolt://localhost:7687"
username = "neo4j"
password = "silviamaca"
driver = GraphDatabase.driver(uri, auth=(username, password))

# Caricamento dataset
df = pd.read_csv("cleaned_dataset.csv", sep=";")

def safe(val):
    return None if pd.isna(val) else val

def create_graph(tx, query, parameters=None):
    tx.run(query, parameters or {})

def main():
    with driver.session() as session:
        for _, row in df.iterrows():
            if pd.isna(row["TipoPersona"]) or str(row["TipoPersona"]).strip() == "":
                continue  # Salta righe senza persona

            protocollo = str(row["Protocollo"])
            persona_id = f"{protocollo}_{row['Progressivo']}"

            # INCIDENTE
            session.execute_write(create_graph, """
                MERGE (i:Incidente {Protocollo: $Protocollo})
                SET i.StazionePolizia = $StazionePolizia,
                    i.DataOraIncidente = $DataOraIncidente,
                    i.NaturaIncidente = $NaturaIncidente,
                    i.Strada = $Strada,
                    i.Feriti = $Feriti,
                    i.Morti = $Morti,
                    i.Illesi = $Illesi,
                    i.Longitudine = $Longitudine,
                    i.Latitudine = $Latitudine,
                    i.Confermato = $Confermato
            """, {
                "Protocollo": protocollo,
                "StazionePolizia": safe(row["StazionePolizia"]),
                "DataOraIncidente": safe(row["DataOraIncidente"]),
                "NaturaIncidente": safe(row["NaturaIncidente"]),
                "Strada": safe(row["Strada"]),
                "Feriti": safe(row["Feriti"]),
                "Morti": safe(row["Morti"]),
                "Illesi": safe(row["Illesi"]),
                "Longitudine": safe(row["Longitudine"]),
                "Latitudine": safe(row["Latitudine"]),
                "Confermato": safe(row["Confermato"]),
            })

            # STRADA (crea nodo specifico per ogni incidente)
            session.execute_write(create_graph, """
                MATCH (i:Incidente {Protocollo: $Protocollo})
                CREATE (s:Strada {
                    nome: $Strada,
                    ClassificazioneStrada: $ClassificazioneStrada,
                    Localizzazione: $Localizzazione,
                    ParticolaritaStrada: $ParticolaritaStrada,
                    TipoStrada: $TipoStrada,
                    FondoStradale: $FondoStradale,
                    Pavimentazione: $Pavimentazione,
                    Segnaletica: $Segnaletica,
                    CondizioneAtmosferica: $CondizioneAtmosferica,
                    Traffico: $Traffico,
                    Visibilita: $Visibilita,
                    Illuminazione: $Illuminazione
                })
                MERGE (i)-[:AVVENUTO_IN]->(s)
            """, {
                "Strada": safe(row["Strada"]),
                "ClassificazioneStrada": safe(row["ClassificazioneStrada"]),
                "Localizzazione": safe(row["Localizzazione"]),
                "ParticolaritaStrada": safe(row["ParticolaritaStrada"]),
                "TipoStrada": safe(row["TipoStrada"]),
                "FondoStradale": safe(row["FondoStradale"]),
                "Pavimentazione": safe(row["Pavimentazione"]),
                "Segnaletica": safe(row["Segnaletica"]),
                "CondizioneAtmosferica": safe(row["CondizioneAtmosferica"]),
                "Traffico": safe(row["Traffico"]),
                "Visibilita": safe(row["Visibilita"]),
                "Illuminazione": safe(row["Illuminazione"]),
                "Protocollo": protocollo
            })

            # PERSONA
            session.execute_write(create_graph, """
                MERGE (p:Persona {PersonaID: $PersonaID})
                SET p.TipoPersona = $TipoPersona,
                    p.Sesso = $Sesso,
                    p.TipoLesione = $TipoLesione,
                    p.DecedutoOspedale = $DecedutoOspedale
                WITH p
                MATCH (i:Incidente {Protocollo: $Protocollo})
                MERGE (p)-[:COINVOLTA_IN]->(i)
            """, {
                "PersonaID": persona_id,
                "TipoPersona": safe(row["TipoPersona"]),
                "Sesso": safe(row["Sesso"]),
                "TipoLesione": safe(row["TipoLesione"]),
                "DecedutoOspedale": safe(row["DecedutoOspedale"]),
                "Protocollo": protocollo
            })

            # VEICOLO
            if pd.notna(row["TipoVeicolo"]) and str(row["TipoVeicolo"]).strip() != "":
                session.execute_write(create_graph, """
                    CREATE (v:Veicolo {
                        TipoVeicolo: $TipoVeicolo,
                        StatoVeicolo: $StatoVeicolo,
                        CinturaCasco: $CinturaCasco,
                        Airbag: $Airbag
                    })
                    WITH v
                    MATCH (i:Incidente {Protocollo: $Protocollo})
                    MERGE (v)-[:PRESENTE_IN]->(i)
                    WITH v
                    MATCH (p:Persona {PersonaID: $PersonaID})
                    MERGE (p)-[:GUIDAVA]->(v)
                """, {
                    "TipoVeicolo": safe(row["TipoVeicolo"]),
                    "StatoVeicolo": safe(row["StatoVeicolo"]),
                    "CinturaCasco": safe(row["Cintura/Casco"]),
                    "Airbag": safe(row["Airbag"]),
                    "Protocollo": protocollo,
                    "PersonaID": persona_id
                })

if __name__ == "__main__":
    main()
    driver.close()
    print("Grafo (modello NON ottimale) creato con successo!")
