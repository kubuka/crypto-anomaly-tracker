import os
from datetime import datetime, timedelta

base_dir = "/opt/data/silver"
retention_period_days = 11


def clean_old_parquet_files():

    if not os.path.exists(base_dir):
        print(f"Directory {base_dir} does not exist.")
        return

    deadline = datetime.now() - timedelta(days=retention_period_days)

    for year_folder in os.listdir(base_dir):
        year_path = os.path.join(base_dir, year_folder)
        if not os.path.isdir(year_path) or not year_folder.startswith("year="):
            continue

        for month_folder in os.listdir(year_path):
            month_path = os.path.join(year_path, month_folder)
            if not os.path.isdir(month_path) or not month_folder.startswith("month="):
                continue

            for day_folder in os.listdir(month_path):
                day_path = os.path.join(month_path, day_folder)
                if not os.path.isdir(day_path) or not day_folder.startswith("day="):
                    continue

                try:
                    y = int(year_folder.split("=")[1])
                    m = int(month_folder.split("=")[1])
                    d = int(day_folder.split("=")[1])
                    folder_date = datetime(y, m, d)

                    if folder_date < deadline:
                        print(f"Deleting old folder: {day_path}")
                        os.system(f"rm -rf {day_path}")

                except (ValueError, IndexError):
                    print(f"Skipping folder with invalid date format: {day_path}")
                    continue

            if not os.listdir(month_path):
                print(f"Deleting empty month folder: {month_path}")
                os.rmdir(month_path)

        if not os.listdir(year_path):
            print(f"Deleting empty year folder: {year_path}")
            os.rmdir(year_path)
