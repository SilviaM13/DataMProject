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
    "CREATE INDEX IF NOT EXISTS FOR (i:Incidente) ON (i.Localizzazione);",

    "CREATE INDEX IF NOT EXISTS FOR (s:Strada) ON (s.nome);",
    "CREATE INDEX IF NOT EXISTS FOR (s:Strada) ON (s.FondoStradale);",
    "CREATE INDEX IF NOT EXISTS FOR (s:Strada) ON (s.Segnaletica);",
    "CREATE INDEX IF NOT EXISTS FOR (s:Strada) ON (s.Traffico);",
    "CREATE INDEX IF NOT EXISTS FOR (s:Strada) ON (s.Visibilita);",
    "CREATE INDEX IF NOT EXISTS FOR (s:Strada) ON (s.TipoStrada);",
    "CREATE INDEX IF NOT EXISTS FOR (s:Strada) ON (s.ClassificazioneStrada);",

    "CREATE INDEX IF NOT EXISTS FOR (p:Persona) ON (p.PersonaID);",
    "CREATE INDEX IF NOT EXISTS FOR (p:Persona) ON (p.TipoPersona);",
    "CREATE INDEX IF NOT EXISTS FOR (p:Persona) ON (p.Sesso);",
    "CREATE INDEX IF NOT EXISTS FOR (p:Persona) ON (p.TipoLesione);",

    "CREATE INDEX IF NOT EXISTS FOR (v:Veicolo) ON (v.TipoVeicolo);",
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
