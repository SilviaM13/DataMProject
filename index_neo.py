from neo4j import GraphDatabase

uri = "bolt://localhost:7687"
username = "neo4j"
password = "silviamaca"

driver = GraphDatabase.driver(uri, auth=(username, password))

index_queries = [

    "CREATE INDEX IF NOT EXISTS FOR (i:Incidente) ON (i.Protocollo);",
    "CREATE INDEX IF NOT EXISTS FOR (i:Incidente) ON (i.DataOraIncidente);",
    "CREATE INDEX IF NOT EXISTS FOR (i:Incidente) ON (i.Morti);",
    "CREATE INDEX IF NOT EXISTS FOR (i:Incidente) ON (i.NaturaIncidente);",

    "CREATE INDEX IF NOT EXISTS FOR (s:Strada) ON (s.nome);",
    "CREATE INDEX IF NOT EXISTS FOR (s:Strada) ON (s.Localizzazione);",
    "CREATE INDEX IF NOT EXISTS FOR (s:Strada) ON (s.Particolarita);",
    "CREATE INDEX IF NOT EXISTS FOR (s:Strada) ON (s.FondoStradale);",
    "CREATE INDEX IF NOT EXISTS FOR (s:Strada) ON (s.Pavimentazione);",
    "CREATE INDEX IF NOT EXISTS FOR (s:Strada) ON (s.Segnaletica);",
    "CREATE INDEX IF NOT EXISTS FOR (s:Strada) ON (s.Traffico);",
    "CREATE INDEX IF NOT EXISTS FOR (s:Strada) ON (s.Illuminazione);",

    "CREATE INDEX IF NOT EXISTS FOR (p:Persona) ON (p.PersonaID);",
    "CREATE INDEX IF NOT EXISTS FOR (p:Persona) ON (p.Sesso);",
    "CREATE INDEX IF NOT EXISTS FOR (p:Persona) ON (p.DecedutoOspedale);",

    "CREATE INDEX IF NOT EXISTS FOR (tp:TipoPersona) ON (tp.nome);",
    "CREATE INDEX IF NOT EXISTS FOR (tl:TipoLesione) ON (tl.nome);",
    "CREATE INDEX IF NOT EXISTS FOR (tv:TipoVeicolo) ON (tv.nome);",
    "CREATE INDEX IF NOT EXISTS FOR (ts:TipoStrada) ON (ts.nome);",
    "CREATE INDEX IF NOT EXISTS FOR (cs:ClassificazioneStrada) ON (cs.nome);",
    "CREATE INDEX IF NOT EXISTS FOR (ca:CondizioneAtmosferica) ON (ca.nome);",
    "CREATE INDEX IF NOT EXISTS FOR (v:Visibilita) ON (v.nome);",

    "CREATE INDEX IF NOT EXISTS FOR (v:Veicolo) ON (v.CinturaCasco);",
    "CREATE INDEX IF NOT EXISTS FOR (v:Veicolo) ON (v.Airbag);"
]

def create_indexes():
    with driver.session() as session:
        for i, query in enumerate(index_queries, 1):
            try:
                session.run(query)
                print(f"Indice {i} creato correttamente.")
            except Exception as e:
                print(f"Errore nella creazione dell'indice {i}: {e}")

if __name__ == "__main__":
    create_indexes()
    driver.close()
