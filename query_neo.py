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
        MATCH (p:Persona)-[:LESIONE]->(tl:TipoLesione),
              (p)-[:GUIDAVA]->(v:Veicolo),
              (p)-[:COINVOLTA_IN]->(i:Incidente)-[:AVVENUTO_IN]->(s:Strada)
        WHERE v.Airbag = 'Esploso'
          AND tl.nome = 'Deceduto'
        RETURN DISTINCT i.DataOraIncidente, i.NaturaIncidente, s.nome AS Strada

    """),

    ("Query 2", """
        MATCH (p:Persona)-[:GUIDAVA]->(veh:Veicolo)-[:TIPO_VEICOLO]->(tv:TipoVeicolo),
              (p)-[:COINVOLTA_IN]->(i:Incidente)-[:AVVENUTO_IN]->(s:Strada),
              (i)-[:HA_VISIBILITA]->(v:Visibilita)
        WHERE v.nome = 'Sufficiente'
            AND s.Traffico = 'Normale'
            AND i.NaturaIncidente = 'Investimento di pedone'
        RETURN tv.nome AS TipoVeicolo, COUNT(DISTINCT i) AS NumeroIncidenti

    """),

    ("Query 3", """
        MATCH (p:Persona)-[:TIPO_PERSONA]->(tp:TipoPersona),
              (p)-[:GUIDAVA]->(v:Veicolo),
              (p)-[:COINVOLTA_IN]->(i:Incidente)-[:AVVENUTO_IN]->(s:Strada)
        WHERE tp.nome = 'Conducente'
        RETURN s.nome AS Strada, s.FondoStradale AS FondoStradale, s.Segnaletica AS Segnaletica, COUNT(DISTINCT i) AS NumeroIncidenti
    """),

    ("Query 4", """
        MATCH (i:Incidente)-[:HA_TIPO]->(ts:TipoStrada)
        MATCH (p:Persona)-[:COINVOLTA_IN]->(i),
              (p)-[:TIPO_PERSONA]->(tp:TipoPersona),
              (p)-[:GUIDAVA]->(:Veicolo)
        WHERE tp.nome = 'Conducente' AND p.Sesso = 'M'
        WITH i, ts.nome AS TipoStrada, COUNT(DISTINCT p) AS num_conducenti
        WHERE num_conducenti >= 2
        RETURN DISTINCT i.Protocollo, TipoStrada, i.DataOraIncidente, i.NaturaIncidente
    """),

    ("Query 5", """
        MATCH (i:Incidente)-[:AVVENUTO_IN]->(s:Strada),
              (p:Persona)-[:COINVOLTA_IN]->(i),
              (p)-[:GUIDAVA]->(v:Veicolo),
              (i)-[:CLASSIFICATA_COME]->(cs:ClassificazioneStrada)
        WHERE cs.nome = 'Strada Urbana'
            AND v.CinturaCasco = 'Non utilizzato'
            AND toInteger(i.Morti) > 0
        WITH s.Localizzazione AS Localizzazione, i.NaturaIncidente AS NaturaIncidente, SUM(toInteger(i.Morti)) AS TotaleMorti
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
            duration = (end - start) * 1000  #ms
            times.append(duration)

            
            dettagli.append({
                "Query": name,
                "Iterazione": i + 1,
                "Tempo_ms": round(duration, 2),
                "DBMS": "Neo4j Ottimale"
            })

            print(f"{name} - Iterazione {i+1}: {duration:.2f} ms")

        media = statistics.mean(times)
        mean.append({
            "Query": name,
            "Media_ms": round(media, 2)
        })


pd.DataFrame(mean).to_csv("mean_times_neo.csv", index=False)
pd.DataFrame(dettagli).to_csv("dettagli_neo.csv", index=False)
print("\nSalvati mean_times_neo.csv e dettagli_neo.csv")

driver.close()