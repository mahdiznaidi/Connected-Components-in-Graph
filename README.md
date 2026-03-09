# Connected Components in Graph (CCF)

Ce depot contient notre implementation Spark de l'algorithme CCF
(Connected Components in Graphs via MapReduce).

Reference du papier:
https://www.cse.unr.edu/~hkardes/pdfs/ccf.pdf

## Ma partie (P2)

Fichiers implementes:

- `ccf_dataframe.py`: implementation complete de CCF en PySpark DataFrame
  - CCF-Iterate
  - CCF-Dedup
  - boucle iterative jusqu'a convergence
  - sortie au format `(node, component_id)`
- `CCFScala.scala`: implementation CCF en Scala Spark RDD
- `validate.py`: script de validation croisee (DataFrame vs RDD de reference, avec module RDD externe optionnel)
- `benchmark_ccf.py`: script de benchmark de scalabilite avec `networkx.gnm_random_graph`
- `outputs/benchmark_results.csv`: sortie benchmark (temps d'execution + iterations)
- `outputs/benchmark_plot.png`: courbe de scalabilite

## Prerequis

- Python 3.9+ (ou compatible)
- PySpark 3.x
- Scala 2.12 + Spark 3.x (pour l'execution Scala)
- Packages Python:
  - `pyspark`
  - `networkx`
  - `matplotlib`

## Lancer la validation

```bash
python validate.py
```

Module RDD externe optionnel:

```bash
python validate.py --external-rdd-module your_module_name
```

## Lancer le benchmark

Tailles par defaut demandees:

```bash
python benchmark_ccf.py --sizes 1000,10000,100000,500000
```

Sorties:

- `outputs/benchmark_results.csv`
- `outputs/benchmark_plot.png`

## Notes

- La generation de graphe pour les benchmarks utilise `networkx.gnm_random_graph`.
- En environnement local contraint, les tres gros runs peuvent necessiter un ajustement
  (`--edge-factor`, partitions Spark, et espace disque disponible).
