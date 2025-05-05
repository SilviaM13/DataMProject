from neo4j import GraphDatabase
import pandas as pd

uri = "bolt://localhost:7687"
username = "neo4j"
password = "silviamaca"

driver = GraphDatabase.driver(uri, auth=(username, password))

def safe(val):
    return str(val).strip() if pd.notna(val) and str(val).strip() != "" else None

df = pd.read_csv("cleaned_dataset.csv", sep=";")

def create_graph(tx, query, parameters=None):
    tx.run(query, parameters or {})

def main():
    with driver.session() as session:
        for _, row in df.iterrows():
            if pd.isna(row["TipoPersona"]) or str(row["TipoPersona"]).strip() == "":
                continue

            protocollo = str(row["Protocollo"])
            persona_id = f"{protocollo}_{row['Progressivo']}"

            # INCIDENTE
            session.execute_write(create_graph, """
                MERGE (i:Incidente {Protocollo: $Protocollo})
                SET i.StazionePolizia = $StazionePolizia,
                    i.DataOraIncidente = $DataOraIncidente,
                    i.NaturaIncidente = $NaturaIncidente,
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
                "Feriti": safe(row["Feriti"]),
                "Morti": safe(row["Morti"]),
                "Illesi": safe(row["Illesi"]),
                "Longitudine": safe(row["Longitudine"]),
                "Latitudine": safe(row["Latitudine"]),
                "Confermato": safe(row["Confermato"]),
            })

            # STRADA
            session.execute_write(create_graph, """
                MATCH (i:Incidente {Protocollo: $Protocollo})
                CREATE (s:Strada {
                    nome: $Strada,
                    Localizzazione: $Localizzazione,
                    Particolarita: $ParticolaritaStrada,
                    FondoStradale: $FondoStradale,
                    Pavimentazione: $Pavimentazione,
                    Segnaletica: $Segnaletica,
                    Traffico: $Traffico,
                    Illuminazione: $Illuminazione
                })
                MERGE (i)-[:AVVENUTO_IN]->(s)
            """, {
                "Protocollo": protocollo,
                "Strada": safe(row["Strada"]),
                "Localizzazione": safe(row["Localizzazione"]),
                "ParticolaritaStrada": safe(row["ParticolaritaStrada"]),
                "FondoStradale": safe(row["FondoStradale"]),
                "Pavimentazione": safe(row["Pavimentazione"]),
                "Segnaletica": safe(row["Segnaletica"]),
                "Traffico": safe(row["Traffico"]),
                "Illuminazione": safe(row["Illuminazione"])
            })

            # NODI CONDIVISI & RELAZIONI
            for node_type, col_name, rel in [
                ("TipoStrada", "TipoStrada", "HA_TIPO"),
                ("ClassificazioneStrada", "ClassificazioneStrada", "CLASSIFICATA_COME"),
                ("CondizioneAtmosferica", "CondizioneAtmosferica", "HA_CONDIZIONE"),
                ("Visibilita", "Visibilita", "HA_VISIBILITA")
            ]:
                val = safe(row[col_name])
                if val:
                    session.execute_write(create_graph, f"""
                        MERGE (x:{node_type} {{nome: $val}})
                        WITH x
                        MATCH (i:Incidente {{Protocollo: $Protocollo}})
                        MERGE (i)-[:{rel}]->(x)
                    """, {
                        "val": val,
                        "Protocollo": protocollo
                    })
                # else:
                #     print(f"[SKIPPED] {col_name} missing for Protocollo {protocollo}: '{row[col_name]}'")

            # PERSONA
            session.execute_write(create_graph, """
                MERGE (p:Persona {PersonaID: $PersonaID})
                SET p.Sesso = $Sesso,
                    p.DecedutoOspedale = $DecedutoOspedale
                MERGE (tp:TipoPersona {nome: $TipoPersona})
                MERGE (tl:TipoLesione {nome: $TipoLesione})
                MERGE (p)-[:TIPO_PERSONA]->(tp)
                MERGE (p)-[:LESIONE]->(tl)
                WITH p
                MATCH (i:Incidente {Protocollo: $Protocollo})
                MERGE (p)-[:COINVOLTA_IN]->(i)
            """, {
                "PersonaID": persona_id,
                "Sesso": safe(row["Sesso"]),
                "DecedutoOspedale": safe(row["DecedutoOspedale"]),
                "TipoPersona": safe(row["TipoPersona"]),
                "TipoLesione": safe(row["TipoLesione"]),
                "Protocollo": protocollo
            })

            # VEICOLO
            tipo_veicolo = safe(row["TipoVeicolo"])
            if tipo_veicolo:
                session.execute_write(create_graph, """
                    CREATE (v:Veicolo {
                        CinturaCasco: $CinturaCasco,
                        Airbag: $Airbag,
                        StatoVeicolo: $StatoVeicolo
                    })
                    MERGE (tv:TipoVeicolo {nome: $TipoVeicolo})
                    MERGE (v)-[:TIPO_VEICOLO]->(tv)
                    WITH v
                    MATCH (i:Incidente {Protocollo: $Protocollo})
                    MERGE (v)-[:PRESENTE_IN]->(i)
                    WITH v
                    MATCH (p:Persona {PersonaID: $PersonaID})
                    MERGE (p)-[:GUIDAVA]->(v)
                """, {
                    "CinturaCasco": safe(row["Cintura/Casco"]),
                    "Airbag": safe(row["Airbag"]),
                    "StatoVeicolo": safe(row["StatoVeicolo"]),
                    "TipoVeicolo": tipo_veicolo,
                    "Protocollo": protocollo,
                    "PersonaID": persona_id
                })

if __name__ == "__main__":
    main()
    driver.close()
    print("Grafo creato con successo!")
