import atexit
from bs4 import BeautifulSoup
from datetime import datetime
import json
from netkeiba_scraper.load import BaseLoader
from multiprocessing import Pool
import os
import re
import requests


def get_latest_races(races, cutoff_date, top_n=3):
    # Define the cutoff date
    cutoff_date = datetime.strptime(cutoff_date, "%Y-%m-%d")

    # Filter races before the cutoff
    filtered_races = [
        race for race in races
        if datetime.strptime(race["race_date"], "%Y-%m-%d") < cutoff_date
    ]

    # Sort by race_date descending (most recent first)
    sorted_races = sorted(
        filtered_races,
        key=lambda r: datetime.strptime(r["race_date"], "%Y-%m-%d"),
        reverse=True
    )

    return sorted_races[:top_n]


def get_horse_data(horse_id, cutoff_date, horse_loader):
    bloodline, race_history = horse_loader.load(horse_id)
    horse_data = dict(bloodline[0])
    race_history = get_latest_races(race_history, cutoff_date)
    for i, race in enumerate(race_history):
        for key, value in race.items():
            horse_data[f'race{i}_{key}'] = value
    return horse_data

result_loader = None
horse_loader = None

def init_result_loader():
    global result_loader
    result_loader = BaseLoader("result")

    def cleanup():
        try:
            result_loader.close()
        except Exception:
            pass
    
    atexit.register(cleanup)

def init_loaders():
    global result_loader, horse_loader
    result_loader = BaseLoader("result")
    horse_loader = BaseLoader("horse")

    def cleanup():
        try:
            result_loader.close()
        except Exception:
            pass
        try:
            horse_loader.close()
        except Exception:
            pass

    atexit.register(cleanup)

def process_race(race_id):
    global result_loader, horse_loader
    rows = []
    try:
        race_entry = result_loader.load(race_id)
        race_info = race_entry[0][0]
        horses = race_entry[1]

        for horse in horses[1:]:
            row = None
            try:
                past_data = get_horse_data(horse["horse_id"], race_info["race_date"], horse_loader)
                row = {**race_info, **horse, **past_data}
            except Exception as e:
                print(f"[ERROR] get_horse_data failed for {horse['horse_id']} on {race_info['race_date']}: {e}", flush=True)
            if row:
                rows.append(row)
    except Exception as e:
        print(f"[ERROR] Failed race {race_id}: {e}", flush=True)

    return rows


def save_race_data(race_ids, output_path="race_data.jsonl", processed_ids_path="processed_race_ids.txt", workers=1):
    # Load saved keys to avoid duplicates
    saved_pairs = set()
    try:
        with open(output_path, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    row = json.loads(line)
                    saved_pairs.add((row["id"], row["horse_id"]))  # adjust key if needed
                except Exception:
                    pass
    except FileNotFoundError:
        pass
    # Load processed race ids to avoid repeat
    processed_race_ids = set()
    try:
        with open(processed_ids_path, "r", encoding="utf-8") as f:
            processed_race_ids = set(line.strip() for line in f if line.strip())
    except FileNotFoundError:
        pass

    race_ids_left = [rid for rid in race_ids if rid not in processed_race_ids]
    total_races = len(race_ids)
    processed_races = len(processed_race_ids)
    print(f"[INFO] Starting to process {total_races} races using {workers} workers...")
    print(f"[INFO] Skipping {processed_races} races already processed.")
    with Pool(workers, initializer=init_loaders) as pool, \
        open(output_path, "a", encoding="utf-8") as f_out, \
        open(processed_ids_path, "a", encoding="utf-8") as f_done:
        for rows in pool.imap_unordered(process_race, race_ids_left):
            for row in rows:
                key = (row["id"], row["horse_id"])
                if key in saved_pairs:
                    continue
                f_out.write(json.dumps(row, ensure_ascii=False) + "\n")
                saved_pairs.add(key)
            f_out.flush()
            f_done.write(rows[0]["id"] + "\n")
            f_done.flush()
            processed_races += 1
            print(f"[PROGRESS] {processed_races}/{total_races} races done.", flush=True)

    print(f"[DONE] Finished saving race data.")

    
