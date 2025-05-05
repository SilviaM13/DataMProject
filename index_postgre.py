import psycopg2

#modifica questi parametri con i tuoi
db_config = {
    "dbname": "DBproject",
    "user": "postgres",
    "password": "silvia",
    "host": "localhost",
    "port": "5432"
}

index_queries = [
    #q1
    "CREATE INDEX IF NOT EXISTS idx_coinvolti_lesione_cintura ON coinvolti (tipolesione, cinturacasco, protocollo, progressivo);",
    #q2
    "CREATE INDEX IF NOT EXISTS idx_strade_ambiente ON strade (visibilita, traffico);",
    "CREATE INDEX IF NOT EXISTS idx_incidenti_natura ON incidenti (naturaincidente);",
    "CREATE INDEX IF NOT EXISTS idx_coinvolti_tipoveicolo ON coinvolti (tipoveicolo, protocollo);",
    #q3
     "CREATE INDEX IF NOT EXISTS idx_strade_condizioni ON strade (strada, fondostradale, segnaletica, protocollo);",
    "CREATE INDEX IF NOT EXISTS idx_coinvolti_conducente ON coinvolti (tipopersona, protocollo, progressivo);",
    #q4
    "CREATE INDEX IF NOT EXISTS idx_coinvolti_conducenti_maschi ON coinvolti (tipopersona, sesso, protocollo, progressivo);",
    "CREATE INDEX IF NOT EXISTS idx_strade_tipostrada ON strade (tipostrada, protocollo);",
    "CREATE INDEX IF NOT EXISTS idx_coinvolti_protocollo ON coinvolti (protocollo);",
    #q5
    "CREATE INDEX IF NOT EXISTS idx_incidenti_natura_morti ON incidenti (naturaincidente, morti);",
    "CREATE INDEX IF NOT EXISTS idx_strade_local_classificazione ON strade (localizzazione, classificazionestrada, protocollo);"

]

def create_indexes():
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                for i, query in enumerate(index_queries, 1):
                    cur.execute(query)
                    print(f"Indice {i} creato correttamente.")
            conn.commit()
    except Exception as e:
        print("Errore nella creazione degli indici:", e)

if __name__ == "__main__":
    create_indexes()