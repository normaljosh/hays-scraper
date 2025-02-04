import os
import json
import argparse
from time import time
from datetime import datetime
import boto3
from bs4 import BeautifulSoup

case_json_path = os.path.join("data", "case_json")

file_list = os.listdir(case_json_path)

# read case ids (first 1000 for now)
all_case_data = {}
for case_filename in file_list[:1000]:
    case_id = os.path.splitext(os.path.basename(case_filename))[0]
    with open(os.path.join(case_json_path, case_filename), "r") as f:
        case_data = json.load(f)
    all_case_data[case_id] = case_data

# export to s3 bucket
case_data_str = json.dumps(all_case_data)
cli = boto3.client("s3")
cli.put_object(
    Body=json.dumps(all_case_data),
    Bucket="indigent-defense",
    Key="case_id_example.json",
)
