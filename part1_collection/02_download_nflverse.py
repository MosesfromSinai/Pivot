import time, os
import nflreadpy as nfl

out = os.path.expanduser("~/nfl_data/raw/nflverse")
os.makedirs(out, exist_ok=True)

start = time.time()
seasons = list(range(2009, 2026))

injuries = nfl.load_injuries(seasons=seasons)
injuries.write_parquet(f"{out}/injuries.parquet")

schedules = nfl.load_schedules(seasons=seasons)
schedules.write_parquet(f"{out}/schedules.parquet")

print("injuries:", injuries.shape)
print("schedules:", schedules.shape)
print(f"Total download time: {time.time() - start:.1f} seconds")
