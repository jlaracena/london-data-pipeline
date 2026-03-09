"""Airflow DAG — forest_pipeline_cloud.

Daily pipeline that triggers Cloud Function extractors, runs dbt via Cloud Run,
and validates the BigQuery raw layer.

Task graph:
    extract_load (TaskGroup, parallel)
    ├── invoke_weather
    ├── invoke_airquality
    ├── invoke_news
    ├── invoke_countries
    ├── invoke_tfl
    ├── invoke_bankholidays
    └── invoke_crime
            │
        run_dbt
            │
       validate_bq
"""

from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.providers.google.cloud.operators.cloud_run import CloudRunJobOperator
from airflow.providers.google.cloud.operators.functions import CloudFunctionsInvokeOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

_GCP_CONN = "google_cloud_default"
_PROJECT = Variable.get("gcp_project_id", default_var="your-project-id")
_REGION = Variable.get("gcp_region", default_var="europe-west2")

default_args = {
    "owner": "forest",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="forest_pipeline_cloud",
    description="Cloud pipeline: Cloud Functions → dbt Cloud Run → BigQuery validation",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["forest", "cloud", "ingestion"],
) as dag:

    with TaskGroup(group_id="extract_load") as extract_load:
        invoke_weather = CloudFunctionsInvokeOperator(
            task_id="invoke_weather",
            function_id=Variable.get("fn_weather_name", default_var="fn-extract-weather"),
            location=_REGION,
            project_id=_PROJECT,
            gcp_conn_id=_GCP_CONN,
        )
        invoke_airquality = CloudFunctionsInvokeOperator(
            task_id="invoke_airquality",
            function_id=Variable.get("fn_airquality_name", default_var="fn-extract-airquality"),
            location=_REGION,
            project_id=_PROJECT,
            gcp_conn_id=_GCP_CONN,
        )
        invoke_news = CloudFunctionsInvokeOperator(
            task_id="invoke_news",
            function_id=Variable.get("fn_news_name", default_var="fn-extract-news"),
            location=_REGION,
            project_id=_PROJECT,
            gcp_conn_id=_GCP_CONN,
        )
        invoke_countries = CloudFunctionsInvokeOperator(
            task_id="invoke_countries",
            function_id=Variable.get("fn_countries_name", default_var="fn-extract-countries"),
            location=_REGION,
            project_id=_PROJECT,
            gcp_conn_id=_GCP_CONN,
        )
        invoke_tfl = CloudFunctionsInvokeOperator(
            task_id="invoke_tfl",
            function_id=Variable.get("fn_tfl_name", default_var="fn-extract-tfl"),
            location=_REGION,
            project_id=_PROJECT,
            gcp_conn_id=_GCP_CONN,
        )
        invoke_bankholidays = CloudFunctionsInvokeOperator(
            task_id="invoke_bankholidays",
            function_id=Variable.get("fn_bankholidays_name", default_var="fn-extract-bankholidays"),
            location=_REGION,
            project_id=_PROJECT,
            gcp_conn_id=_GCP_CONN,
        )
        invoke_crime = CloudFunctionsInvokeOperator(
            task_id="invoke_crime",
            function_id=Variable.get("fn_crime_name", default_var="fn-extract-crime"),
            location=_REGION,
            project_id=_PROJECT,
            gcp_conn_id=_GCP_CONN,
        )

    run_dbt = CloudRunJobOperator(
        task_id="run_dbt",
        project_id=_PROJECT,
        region=_REGION,
        job_name=Variable.get("dbt_cloud_run_job", default_var="dbt-run"),
        gcp_conn_id=_GCP_CONN,
    )

    validate_bq = BigQueryCheckOperator(
        task_id="validate_bq",
        sql=(
            "SELECT COUNT(*) FROM `{{ var.value.gcp_project_id }}.raw.raw_weather` "
            "WHERE DATE(ingested_at) = CURRENT_DATE()"
        ),
        use_legacy_sql=False,
        gcp_conn_id=_GCP_CONN,
    )

    extract_load >> run_dbt >> validate_bq
