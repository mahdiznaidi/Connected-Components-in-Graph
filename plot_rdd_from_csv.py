import csv
from pathlib import Path
import matplotlib.pyplot as plt

# Fichiers
csv_file = Path("outputs/benchmark_results_rdd.csv")
plot_file = Path("outputs/benchmark_plot_rdd.png")

nodes = []
times = []
iterations = []

# Lecture du CSV
with csv_file.open("r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        if row["status"] == "ok":
            nodes.append(int(row["num_nodes"]))
            times.append(float(row["elapsed_seconds"]))
            iterations.append(int(row["iterations"]))

if not nodes:
    print("Aucune ligne valide à tracer.")
else:
    fig, ax1 = plt.subplots(figsize=(12, 7))

    # Courbe temps d'exécution (bleu)
    ax1.plot(
        nodes,
        times,
        marker="o",
        linewidth=2,
        color="tab:blue",
        label="Execution time (s)"
    )

    ax1.set_xlabel("Number of nodes")
    ax1.set_ylabel("Execution time (seconds)", color="tab:blue")
    ax1.set_xscale("log")
    ax1.grid(True, linestyle="--", alpha=0.35)

    # Deuxième axe Y pour les itérations (orange)
    ax2 = ax1.twinx()

    ax2.plot(
        nodes,
        iterations,
        marker="s",
        linewidth=2,
        color="tab:orange",
        label="Iterations"
    )

    ax2.set_ylabel("Iterations", color="tab:orange")

    # Fusion des légendes
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()

    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")

    plt.title("CCF RDD Scalability Benchmark")

    plt.tight_layout()
    plt.savefig(plot_file, dpi=150)
    plt.close()

    print(f"Saved plot: {plot_file.resolve()}")