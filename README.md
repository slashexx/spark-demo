# spark-demo — USGS earthquake analytics with PySpark + Grafana

Analyse the last 30 days of worldwide earthquake activity from the US
Geological Survey, with **PySpark** for the data work and **Grafana**
for the dashboard.

- **Dataset:** `https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.csv`, ~11,000 events worldwide, updated every few minutes.
- **Stack:** Python + PySpark (local mode) → SQLite → Grafana.
- **Runtime:** `python analyze.py` in ~10 seconds, Grafana boots in ~15 s.

## Pipeline

```
   USGS CSV (10k+ rows)
          │
          ▼
  PySpark DataFrame
          │
  parse + aggregate
          │
          ▼
    SQLite (quakes.db)
          │
          ▼
        Grafana
     (6 panels)
```

## Setup

Java 17 and Python 3.11 are required (PySpark 3.5 doesn't support 3.12+):

```bash
brew install openjdk@17 python@3.11
sudo ln -sfn /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk \
             /Library/Java/JavaVirtualMachines/openjdk-17.jdk
python3.11 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

## Run

```bash
# 1. refresh the dataset (optional; a copy is already in data/)
curl -fsSL -o data/quakes.csv \
    https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.csv

# 2. run the PySpark analysis — writes output/quakes.db
python analyze.py

# 3. bring up Grafana (reads output/quakes.db)
docker compose up -d
open http://localhost:3000/d/usgs-quakes
```

The Grafana container loads the `frser-sqlite-datasource` plugin on
first boot (~10 s). The dashboard is provisioned on disk — no UI
clicking needed.

## Dashboard panels

| Panel | SQLite query | What it shows |
|---|---|---|
| Total events | `SELECT total_events FROM summary` | Number of earthquakes in the 30-day window |
| Max / avg magnitude | `SELECT max_mag, avg_mag FROM summary` | Headline magnitude stats |
| Avg depth | `SELECT avg_depth_km FROM summary` | Average hypocentre depth |
| Distinct regions | `SELECT distinct_regions FROM summary` | Unique place-name regions that had at least one event |
| Earthquakes per day | `SELECT day, count FROM daily_counts` | Time-series bar chart of daily activity |
| Magnitude distribution | `SELECT bucket, count FROM mag_buckets` | Histogram of magnitudes bucketed by integer |
| Top 10 strongest | `top_strongest` table | Time, magnitude, depth, place for the strongest quakes |
| Top 10 regions | `top_places` table | Regions with the most events |

## What PySpark is doing

- `SparkSession.builder.master("local[*]")` — start Spark locally, use
  all cores. Same code runs unchanged on a YARN / Kubernetes cluster.
- Explicit `StructType` schema — skip inference for determinism and
  speed.
- Filter (`type == "earthquake"`), then six aggregations via
  `groupBy().agg()` — classic DataFrame API, all lazy, all executed in
  a single physical plan when the first `.toPandas()` call lands.
- `to_date`, `floor`, `regexp_extract`, `countDistinct` — all Spark
  SQL functions, no Python UDFs (so they run in the JVM, fast).
- Final `.toPandas()` round-trip is cheap because each result table
  has at most a few hundred rows.

## Talking points for Q&A

- **Why Spark and not pandas?** Same DataFrame API, but Spark's query
  engine and schema-first approach scale to billions of rows. We ran
  `local[*]` here; flipping to a real cluster needs zero code changes.
- **Why SQLite between Spark and Grafana?** Because Spark writes the
  fact tables once and Grafana just queries them — no streaming
  needed. For streaming we'd swap SQLite for Kafka + a time-series DB.
- **How is an "earthquake" defined?** The USGS `type` field — we
  filter out explosions, quarry blasts, ice quakes, etc.
- **Why integer-bucket magnitude histogram?** The Richter scale is
  logarithmic, so integer bins naturally group orders of magnitude.
- **What's the worst event in the dataset?** The `top_strongest`
  table — the dashboard shows the top 10.

## Files

```
spark-demo/
├── analyze.py                     # PySpark script
├── requirements.txt               # pyspark, pandas, numpy
├── docker-compose.yml             # Grafana only
├── data/quakes.csv                # downloaded USGS feed
├── output/quakes.db               # produced by analyze.py
├── grafana/
│   ├── provisioning/datasources/sqlite.yml
│   ├── provisioning/dashboards/default.yml
│   └── dashboards/earthquakes.json
└── README.md
```
