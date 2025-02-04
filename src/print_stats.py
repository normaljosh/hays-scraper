import os
import json

from time import time
from statistics import mean, median, mode

N_LONGEST = 5
START_TIME = time()

case_data_list = []

case_json_path = os.path.join("data", "case_json")
for case_file in os.scandir(case_json_path):
    with open(case_file.path, "r") as file_handle:
        case_data_list.append(json.loads(file_handle.read()))


def print_top_cases_by_lambda(sort_function, description):
    print("\n", description)
    cases_by_lambda = sorted(case_data_list, key=sort_function)[-N_LONGEST:]
    converted_data = list(sort_function(case) for case in case_data_list)
    print(
        "\n".join(
            f"{i}. {sort_function(case)}".ljust(20) + case["odyssey id"]
            for i, case in enumerate(cases_by_lambda[::-1], 1)
        ),
        "\nMean:",
        round(mean(converted_data), 2),
        " Median:",
        round(median(converted_data), 2),
        " Mode:",
        round(mode(converted_data), 2),
    )


disposition_len = (lambda case: len(case["dispositions"]), "dispositions length")
charges_len = (lambda case: len(case["charge information"]), "number of charges")
events_len = (
    lambda case: len(case["other events and hearings"]),
    "other events and hearings length",
)
case_cost = (
    lambda case: float(
        case["financial information"]["total financial assessment"].replace(",", "")
    )
    if "financial information" in case
    else 0.0,
    "highest cost",
)
for sort_function, description in (events_len, disposition_len, case_cost, charges_len):
    print_top_cases_by_lambda(
        sort_function,
        description,
    )
print("\nNumber of cases:", len(case_data_list))
print("Stats parsing runtime:", round(time() - START_TIME, 2), "seconds")
