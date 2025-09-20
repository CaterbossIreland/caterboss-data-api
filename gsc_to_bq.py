import os, datetime, pandas as pd
from googleapiclient.discovery import build
from google.oauth2 import service_account
from google.cloud import bigquery

SITE    = os.environ["GSC_SITE_URL"]
PROJECT = os.environ["GCP_PROJECT"]
DATASET = os.environ["BQ_DATASET"]
TABLE   = "gsc_search"
SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly","https://www.googleapis.com/auth/bigquery"]

def fetch(service, start, end):
    body = {"startDate":start.strftime("%Y-%m-%d"),"endDate":end.strftime("%Y-%m-%d"),
            "dimensions":["date","page","query","device","country"],"rowLimit":25000}
    resp = service.searchanalytics().query(siteUrl=SITE, body=body).execute()
    rows=[]
    for r in resp.get("rows",[]):
        k=r["keys"]; rows.append({"date":k[0],"page":k[1],"query":k[2],"device":k[3],"country":k[4],
                                  "clicks":r.get("clicks",0),"impressions":r.get("impressions",0),
                                  "ctr":r.get("ctr",0.0),"position":r.get("position",0.0)})
    return pd.DataFrame(rows)

def main():
    creds = service_account.Credentials.from_service_account_file(
        os.environ.get("GOOGLE_APPLICATION_CREDENTIALS","sa.json"), scopes=SCOPES)
    svc = build("searchconsole","v1", credentials=creds)
    today = datetime.date.today()
    start = today - datetime.timedelta(days=2)
    end   = today - datetime.timedelta(days=1)
    df = fetch(svc, start, end)
    if df.empty: print("No rows"); return
    bq = bigquery.Client(project=PROJECT, credentials=creds)
    table_id=f"{PROJECT}.{DATASET}.{TABLE}"
    schema=[bigquery.SchemaField("date","DATE"),bigquery.SchemaField("page","STRING"),
            bigquery.SchemaField("query","STRING"),bigquery.SchemaField("device","STRING"),
            bigquery.SchemaField("country","STRING"),bigquery.SchemaField("clicks","INT64"),
            bigquery.SchemaField("impressions","INT64"),bigquery.SchemaField("ctr","FLOAT64"),
            bigquery.SchemaField("position","FLOAT64")]
    job_config = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_APPEND",
                                        time_partitioning=bigquery.TimePartitioning(field="date"))
    bq.load_table_from_dataframe(df, table_id, job_config=job_config).result()
    print(f"Loaded {len(df)} rows into {table_id}")

if __name__=="__main__":
    main()
