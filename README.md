# Turf vs. Grass: NFL Injury Analysis

CS 179G, UC Riverside, Fall 2026. We use Apache Spark to test whether NFL players get injured more often on synthetic turf than on natural grass. We combine player tracking, play, and injury data, control for factors like position, weather, and play type, and present the results in a web interface.

## Team

- Moses Avila
- Colin Pham
- Joshua Mena
- Adolfo Magallanes

## Data Sources

- **Kaggle: NFL 1st and Future - Analytics**: player tracking, play, and injury records by playing surface.
- **nflverse**: weekly injury reports and game schedules (via `nflreadpy`).

Data lives on the server at `/home/cs179g/nfl_data` and is never committed.

## Repo Structure

```
part1_collection/   # data collection and cleaning
part2_spark/        # Spark analysis
part3_web/          # web interface for results
```

## Setup

```bash
conda activate cs179
pip install kaggle nflreadpy polars pyarrow
```

## Part 2: Spark → MySQL

- MySQL runs on the server at 127.0.0.1:3307, database `pivot`, user `root`, no password. If it's down, start it with `~/start_mysql.sh`.
- Create the tables:

  ```bash
  mysql -u root -h 127.0.0.1 -P 3307 < part2_spark/schema.sql
  ```

- Run the jobs:

  ```bash
  spark-submit --packages com.mysql:mysql-connector-j:8.4.0 part2_spark/01_injury_rates.py
  spark-submit --packages com.mysql:mysql-connector-j:8.4.0 part2_spark/02_movement_features.py
  spark-submit --packages com.mysql:mysql-connector-j:8.4.0 part2_spark/03_league_injury_rates.py
  ```

- The 1 vs 2 worker timing runs and graph are documented in [part2_spark/TIMING.md](part2_spark/TIMING.md).

## Part 3: Web App

- Install once: `pip install -r part3_web/requirements.txt` (inside `conda activate cs179`).
- Start it on the server (MySQL has to be running):

  ```bash
  PORT=5000 python part3_web/app.py
  ```

- `PORT` picks the port (default 5000). If someone else is already using 5000, pick another one like 5001.
- The app only listens on 127.0.0.1, so view it from your laptop with port forwarding, then open http://localhost:5000:

  ```bash
  ssh -L 5000:localhost:5000 cs179g
  ```

- To add a page, drop a module in `part3_web/pages/` with a Blueprint named `bp` and a view called `index`. Set `NAV_TITLE` to show it in the nav. No need to touch `app.py`.
