import psycopg2
import time
import pandas as pd
import statistics

#configurazione
conn_params = {
    "dbname": "DBproject",
    "user": "postgres",
    "password": "silvia",
    "host": "localhost",
    "port": "5432"
}


queries = [
    ("Query 1", """
        SELECT DISTINCT i.dataoraincidente, i.naturaincidente, i.strada
        FROM incidenti i
        JOIN coinvolti c ON i.protocollo = c.protocollo
        WHERE c.airbag = 'Esploso' AND c.tipolesione = 'Deceduto';
    """),

    ("Query 2", """
        SELECT c.tipoveicolo, COUNT(*) AS numero_incidenti
        FROM incidenti i
        JOIN strade s ON i.protocollo = s.protocollo
        JOIN coinvolti c ON i.protocollo = c.protocollo
        WHERE s.visibilita = 'Sufficiente'
        AND s.traffico = 'Normale'
        AND i.naturaincidente = 'Investimento di pedone'
        AND c.tipoveicolo IS NOT NULL
        AND c.tipoveicolo <> ''
        GROUP BY c.tipoveicolo;

    """),
   

    ("Query 3", """
        SELECT s.strada, s.fondostradale, s.segnaletica, COUNT(DISTINCT i.protocollo) AS numero_incidenti
        FROM incidenti i
        JOIN strade s ON i.protocollo = s.protocollo
        JOIN coinvolti c ON i.protocollo = c.protocollo
        WHERE c.tipopersona = 'Conducente' 
        GROUP BY s.strada, s.fondostradale, s.segnaletica;
    """),

    ("Query 4", """
        SELECT DISTINCT i.protocollo, s.tipostrada, i.dataoraincidente, i.naturaincidente
        FROM incidenti i
        JOIN strade s ON i.protocollo = s.protocollo
        JOIN coinvolti c ON i.protocollo = c.protocollo
        WHERE c.tipopersona = 'Conducente' AND c.sesso = 'M'
        GROUP BY i.protocollo, s.tipostrada, i.dataoraincidente, i.naturaincidente
        HAVING COUNT(c.protocollo) >= 2;
    """),

    ("Query 5", """
        SELECT s.localizzazione, i.naturaincidente, SUM(i.morti) AS totale_morti
        FROM incidenti i
        JOIN strade s ON i.protocollo = s.protocollo
        JOIN coinvolti c ON i.protocollo = c.protocollo
        WHERE s.classificazionestrada = 'Strada Urbana'
        AND c.cinturacasco = 'Non utilizzato'
        GROUP BY s.localizzazione, i.naturaincidente
        HAVING SUM(i.morti) > 0;

    """)
]


ITERATIONS = 10
mean = []
dettagli = []

with psycopg2.connect(**conn_params) as conn:
    cur = conn.cursor()
    for name, query in queries:
        times = []
        for i in range(ITERATIONS):
            start = time.perf_counter()
            cur.execute(query)
            cur.fetchall()
            end = time.perf_counter()
            duration = (end - start) * 1000
            times.append(duration)

            dettagli.append({
                "Query": name,
                "Iterazione": i + 1,
                "Tempo_ms": round(duration, 2),
                "DBMS": "PostgreSQL"
            })
            print(f"{name} - Iterazione {i+1}: {duration:.2f} ms")

        media = statistics.mean(times)
        mean.append({
            "Query": name,
            "Media_ms": round(media, 2)
        })


pd.DataFrame(mean).to_csv("mean_times_postgre.csv", index=False)
pd.DataFrame(dettagli).to_csv("dettagli_postgre.csv", index=False)
print("\nSalvati mean_times_postgre.csv e dettagli_postgre.csv")