"""
Combine hearing & event records from multiple case files into a few csvs.
"""
import pandas as pd
from datetime import datetime
import json
import os
import boto3
from io import StringIO

FILE_DIR = os.path.join("data", "case_json")  # relative location of JSON case files
chunk_size = 10000


def parse_event_date(date_str):
    """Return a python `datetime` from e.g. '01/30/2021'"""
    month, day, year = date_str.split("/")
    return datetime(year=int(year), month=int(month), day=int(day))


def iso_event_date(dt):
    """Format a `datetime` instance as YYYY-MM-DD"""
    return dt.strftime("%Y-%m-%d")


def get_days_elapsed(start, end):
    """Return the number of days between two dates"""
    delta = end - start
    return delta.days


def write_list_to_csv(file_list: list, Key: str, bucket: str = "indigent-defense"):
    """
    Write a list of json to the csv
    """
    csv_buffer = StringIO()
    df = pd.DataFrame(file_list)
    df.to_csv(csv_buffer)
    cli = boto3.client("s3")
    cli.put_object(
        Body=json.dumps(file_list),
        Bucket="indigent-defense",
        Key=Key,
    )
    return f"s3://{bucket}/{Key}"


files = [file for file in os.listdir(FILE_DIR) if file.endswith(".json")]

n_files = len(files)
for chunk_start in range(0, n_files, chunk_size):
    chunk_end = chunk_start + chunk_size
    n_chunk = chunk_start // chunk_size
    print(f"processing chunk {n_chunk} of {n_files //chunk_size}, {chunk_size} files")
    events = []
    charges = []
    for f_name in files[chunk_start:chunk_end]:
        with open(f"{FILE_DIR}/{f_name}", "r") as fin:
            """
            Extract fields of interest. you can add any attributes of interest to the
            event_record dict and they will be included in the output CSV.
            Extracts events and charges from the case file, in seperate files.
            """
            case = json.load(fin)

            # extract demographic info
            case_id = case["odyssey id"]
            case_number = case["code"]
            retained = case["party information"]["appointed or retained"]
            gender = case["party information"]["sex"]
            race = case["party information"]["race"]
            defense_attorney = case["party information"]["defense attorney"]

            # extract event data
            first_event_date = None
            for i, event in enumerate(case["other events and hearings"]):
                event_record = {}
                event_date = parse_event_date(event[0])

                if i == 0:
                    first_event_date = event_date

                days_elapsed = get_days_elapsed(first_event_date, event_date)
                event_record["event_id"] = i + 1
                event_record["event_date"] = iso_event_date(event_date)
                event_record["first_event_date"] = iso_event_date(first_event_date)
                event_record["days_elapsed"] = days_elapsed
                event_record["event_name"] = event[1]
                event_record["attorney"] = retained
                event_record["case_id"] = case_id
                event_record["case_number"] = case_number
                event_record["defense_attorney"] = defense_attorney
                event_record["race"] = race
                event_record["gender"] = gender
                events.append(event_record)

            # extract charge data
            for i, charge in enumerate(case["charge information"]):
                charge_record = {}
                charge_record["charge_id"] = i + 1
                charge_record["charge_name"] = charge.get("charges", "")
                charge_record["statute"] = charge.get("statute", "")
                charge_record["level"] = charge.get("level", "")

                charge_record["charge_date"] = charge.get("date", "")
                if charge_record["charge_date"]:
                    charge_record["charge_date"] = iso_event_date(
                        parse_event_date(charge_record["charge_date"])
                    )

                charge_record["case_id"] = case_id
                charge_record["case_number"] = case_number
                charges.append(charge_record)
    # Write events
    Key = f"csv_data/events_combined_{n_chunk}.csv"
    write_list_to_csv(file_list=events, Key=Key, bucket="indigent-defense")

    # Write charges
    Key = f"csv_data/charges_combined_{n_chunk}.csv"
    write_list_to_csv(file_list=charges, Key=Key, bucket="indigent-defense")
