import os
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
from google.cloud import bigquery

API_KEY = os.environ.get("API_KEY","")
PROJECT = os.environ.get("GCP_PROJECT","")
DATASET = os.environ.get("BQ_DATASET","")

app = FastAPI(title="Caterboss Data API")

ALLOWED = {
    "ga4_channel_monthly": f"""SELECT * FROM `{PROJECT}.{DATASET}.v_ga4_monthly_channel`
      WHERE month BETWEEN @start AND @end ORDER BY month, channel""",
    "ga4_landing_categories": f"""SELECT * FROM `{PROJECT}.{DATASET}.v_ga4_monthly_landing`
      WHERE month BETWEEN @start AND @end AND landing_url LIKE '%/category/%'
      ORDER BY month, sessions DESC""",
    "ga4_landing_products": f"""SELECT * FROM `{PROJECT}.{DATASET}.v_ga4_monthly_landing`
      WHERE month BETWEEN @start AND @end AND landing_url LIKE '%/product/%'
      ORDER BY month, sessions DESC""",
    "ga4_srcmed_monthly": f"""SELECT * FROM `{PROJECT}.{DATASET}.v_ga4_monthly_srcmed`
      WHERE month BETWEEN @start AND @end ORDER BY month, sessions DESC""",
    "ga4_device_monthly": f"""SELECT * FROM `{PROJECT}.{DATASET}.v_ga4_monthly_device`
      WHERE month BETWEEN @start AND @end ORDER BY month, device_category""",
    "gsc_categories": f"""SELECT DATE_TRUNC(date,MONTH) AS month,
             SUM(clicks) clicks, SUM(impressions) impressions, AVG(position) pos
      FROM `{PROJECT}.{DATASET}.gsc_search`
      WHERE REGEXP_CONTAINS(page, r'/category/') AND date BETWEEN @start AND @end
      GROUP BY month ORDER BY month"""
}

class RunNamed(BaseModel):
    name: str
    params: dict

bq = bigquery.Client()

@app.post("/run")
def run_named(body: RunNamed, authorization: str = Header("")):
    if not API_KEY or authorization != f"Bearer {API_KEY}":
        raise HTTPException(401, "Unauthorized")
    sql = ALLOWED.get(body.name)
    if not sql:
        raise HTTPException(400, f"Unknown query name: {body.name}")
    params = []
    for k, v in (body.params or {}).items():
        typ = "STRING"
        if isinstance(v, (int,float)): typ = "FLOAT64"
        if k.lower() in ("start","end","start_date","end_date"): typ = "STRING"
        params.append(bigquery.ScalarQueryParameter(k, typ, v))
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    rows = bq.query(sql, job_config=job_config).result()
    return {"rows":[dict(r) for r in rows]}
