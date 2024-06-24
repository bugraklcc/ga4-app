import csv
import json
import pandas as pd
from google.auth.transport.requests import Request
from google.auth.credentials import Credentials
from google.auth.exceptions import RefreshError
from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Dimension, Metric, OrderBy
from src.Enricher.queries import Queries
from src.Enricher.enricher import Enricher

def read_config(config_path="src/json/config.json"):
    with open(config_path, "r") as config_file:
        config_data = json.load(config_file)
    return config_data

def pre_execute(view_id, start_date, end_date, csv_path, query, enrich=True):
    try:
        credentials = service_account.Credentials.from_service_account_file(
            "src/config.json")
        client = BetaAnalyticsDataClient(credentials=credentials)
        print(client)

        request_json = query(start_date, end_date)

        dimensions = [Dimension(name=dimension["name"]) for dimension in request_json["dimensions"]]
        metrics = [Metric(name=metric["name"]) for metric in request_json["metrics"]]
        date_ranges = [DateRange(start_date=request_json["dateRanges"][0]["startDate"],
                                 end_date=request_json["dateRanges"][0]["endDate"])]

        request = RunReportRequest(
            property=f"properties/{view_id}",
            dimensions=dimensions,
            metrics=metrics,
            date_ranges=date_ranges,
            metric_aggregations=["TOTAL"]
        )

        response = client.run_report(request)

        with open(csv_path, 'w', newline='', encoding='utf-8') as csv_file:
            csv_writer = csv.writer(csv_file)

            all_headers = [column.name for column in response.dimension_headers] + [metric.name for metric in
                                                                                    response.metric_headers]
            csv_writer.writerow(all_headers)

            for row in response.rows:
                csv_writer.writerow(
                    [val.value for val in row.dimension_values] +
                    [val.value for val in row.metric_values]
                )

        print(f"Report data written to: {csv_path}")

        if enrich:

            df = pd.read_csv(csv_path)

            df1 = pd.read_csv(output_csv_path_1)


            df1.to_excel("src/csv/ga4_report.xlsx", index=False)

            print("Success: Excel file created.")

    except RefreshError as refresh_error:
        print(f"Error: Credentials expired. Please refresh your credentials. Error details: {refresh_error}")
    except Exception as ex:
        print(f"An unexpected error occurred: {ex}")

if __name__ == "__main__":
    config = read_config()

    view_id = config["view_id"]
    start_date = config["start_date"]
    end_date = config["end_date"]
    output_csv_path_1 = "src/csv/ga4_report.csv"

    query_1 = Queries.get_query_3

    pre_execute(view_id, start_date, end_date, output_csv_path_1, query_1)
