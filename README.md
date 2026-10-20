# Turf vs. Grass: NFL Injury Analysis

CS 179G, UC Riverside, Fall 2026. We use Apache Spark to test whether NFL players get injured more often on synthetic turf than on natural grass. We combine player tracking, play, and injury data, control for factors like position, weather, and play type, and present the results in a web interface.

## Team

- TODO
- TODO
- TODO
- TODO

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
