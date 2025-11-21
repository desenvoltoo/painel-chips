from google.cloud import bigquery
import os

def get_bq_client():
    return bigquery.Client(project=os.getenv("GCP_PROJETO"))
