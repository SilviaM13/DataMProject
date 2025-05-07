from neo4j import GraphDatabase
import time
import pandas as pd
import statistics


uri = "bolt://localhost:7687"
username = "neo4j"
password = "silviamaca"

driver = GraphDatabase.driver(uri, auth=(username, password))

queries = [
    ("Query 1", """
        MATCH (i:Incidente)-[:AVVENUTO_IN]->(s:Strada),
              (p:Persona)-[:COINVOLTA_IN]->(i),
              (p)-[:GUIDAVA]->(v:Veicolo)
        WHERE v.Airbag = 'Esploso'
          AND p.TipoLesione = 'Deceduto'
        RETURN DISTINCT i.DataOraIncidente, i.NaturaIncidente, s.nome AS Strada
    """),

    ("Query 2", """
        MATCH (i:Incidente)-[:AVVENUTO_IN]->(s:Strada),
              (p:Persona)-[:COINVOLTA_IN]->(i),
              (p)-[:GUIDAVA]->(veh:Veicolo)
        
        WHERE s.Visibilita = 'Sufficiente'
          AND s.Traffico = 'Normale'
          AND i.NaturaIncidente = 'Investimento di pedone'
        RETURN veh.TipoVeicolo AS TipoVeicolo, COUNT(DISTINCT i) AS NumeroIncidenti
    """),


    ("Query 3", """
        MATCH (i:Incidente)-[:AVVENUTO_IN]->(s:Strada),
              (p:Persona)-[:COINVOLTA_IN]->(i),
              (p)-[:GUIDAVA]->(v:Veicolo)
        WHERE p.TipoPersona = 'Conducente'
        RETURN s.nome AS Strada, s.FondoStradale, s.Segnaletica, COUNT(DISTINCT i) AS NumeroIncidenti
    """),

    ("Query 4", """
        MATCH (i:Incidente)-[:AVVENUTO_IN]->(s:Strada),
              (p:Persona)-[:COINVOLTA_IN]->(i),
              (p)-[:GUIDAVA]->(v:Veicolo)
        WHERE p.TipoPersona = 'Conducente'
          AND p.Sesso = 'M'
        WITH i, s.TipoStrada AS TipoStrada, COUNT(DISTINCT p) AS num_conducenti
        WHERE num_conducenti >= 2
        RETURN DISTINCT i.Protocollo, TipoStrada, i.DataOraIncidente, i.NaturaIncidente

    """),

    ("Query 5", """
        MATCH (i:Incidente)-[:AVVENUTO_IN]->(s:Strada),
              (p:Persona)-[:COINVOLTA_IN]->(i),
              (p)-[:GUIDAVA]->(v:Veicolo)
        WHERE s.ClassificazioneStrada = 'Strada Urbana'
            AND v.CinturaCasco = 'Non utilizzato'
        WITH s.Localizzazione AS Localizzazione, i.NaturaIncidente AS NaturaIncidente, SUM(i.Morti) AS TotaleMorti
        WHERE TotaleMorti > 0
        RETURN Localizzazione, NaturaIncidente, TotaleMorti
    """)
]

ITERATIONS = 10
mean = []
dettagli = []

with driver.session() as session:
    for name, query in queries:
        times = []
        for i in range(ITERATIONS):
            start = time.perf_counter()
            session.run(query).consume()
            end = time.perf_counter()
            duration = (end - start) * 1000
            times.append(duration)

            dettagli.append({
                "Query": name,
                "Iterazione": i + 1,
                "Tempo_ms": round(duration, 2),
                "DBMS": "Neo4j Non Ottimale"
            })

            print(f"{name} - Iterazione {i+1}: {duration:.2f} ms")

        media = statistics.mean(times)
        mean.append({
            "Query": name,
            "Media_ms": round(media, 2)
        })

pd.DataFrame(mean).to_csv("mean_times_neo_no_opt.csv", index=False)
pd.DataFrame(dettagli).to_csv("dettagli_neo_no_opt.csv", index=False)
print("\nSalvati mean_times_neo_no_opt.csv e dettagli_neo_no_opt.csv")

driver.close()