import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

neo = pd.read_csv("dettagli_neo.csv")
neo_no_opt = pd.read_csv("dettagli_neo_no_opt.csv")
postgre = pd.read_csv("dettagli_postgre.csv")

combined = pd.concat([neo, neo_no_opt, postgre], ignore_index=True)

#grafico x query
query_labels = combined['Query'].unique()

for query in query_labels:
    df_query = combined[combined['Query'] == query].copy()

    plt.figure(figsize=(10, 6))
    sns.lineplot(data=df_query, x="Iterazione", y="Tempo_ms", hue="DBMS", marker="o")

    plt.title(f"{query} - Tempi di esecuzione per iterazione")
    plt.xlabel("Iterazione")
    plt.ylabel("Tempo (ms)")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(f"grafico_{query.replace(' ', '_').lower()}.png")
    plt.show()
