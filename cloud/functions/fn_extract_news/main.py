"""Cloud Function — fn-extract-news.

HTTP-triggered function that extracts London micro-mobility news from NewsAPI
and loads them into BigQuery via a MERGE deduplication strategy.
NEWS_API_KEY is injected from Secret Manager via the environment.
"""

import hashlib
import logging
import os
from datetime import datetime, timezone
from typing import Any

import functions_framework
import httpx
from google.cloud import bigquery
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_NEWSAPI_URL = "https://newsapi.org/v2/everything"
_SOURCE = "newsapi"
_TABLE = "raw_news"


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code >= 500
    return isinstance(exc, (httpx.ConnectError, httpx.TimeoutException, httpx.NetworkError))


@retry(
    retry=retry_if_exception(_is_retryable),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    stop=stop_after_attempt(3),
    reraise=True,
)
def _extract(api_key: str) -> list[dict[str, Any]]:
    """Fetch news articles from NewsAPI."""
    ingested_at = datetime.now(timezone.utc).isoformat()
    params: dict[str, Any] = {
        "q": "London electric bike OR micro-mobility OR forest ebikes",
        "sortBy": "publishedAt",
        "pageSize": 20,
        "language": "en",
    }
    with httpx.Client(timeout=30) as client:
        resp = client.get(_NEWSAPI_URL, params=params, headers={"X-Api-Key": api_key})
        resp.raise_for_status()
        data = resp.json()

    records: list[dict[str, Any]] = []
    for article in data.get("articles", []):
        url: str | None = article.get("url")
        title: str | None = article.get("title")
        if not url or not title:
            continue
        records.append(
            {
                "ingested_at": ingested_at,
                "article_id": hashlib.md5(url.encode()).hexdigest(),
                "title": title,
                "description": article.get("description"),
                "url": url,
                "published_at": article.get("publishedAt"),
                "source_name": (article.get("source") or {}).get("name"),
                "_source": _SOURCE,
            }
        )
    return records


def _load_to_bq(records: list[dict[str, Any]], project: str, dataset: str) -> int:
    """Insert records into BigQuery using MERGE to deduplicate on (article_id)."""
    client = bigquery.Client(project=project)
    staging_table = f"{project}.{dataset}._staging_{_TABLE}"
    target_table = f"{project}.{dataset}.{_TABLE}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
            bigquery.SchemaField("ingested_at", "TIMESTAMP"),
            bigquery.SchemaField("article_id", "STRING"),
            bigquery.SchemaField("title", "STRING"),
            bigquery.SchemaField("description", "STRING"),
            bigquery.SchemaField("url", "STRING"),
            bigquery.SchemaField("published_at", "TIMESTAMP"),
            bigquery.SchemaField("source_name", "STRING"),
            bigquery.SchemaField("_source", "STRING"),
        ],
    )
    client.load_table_from_json(records, staging_table, job_config=job_config).result()

    merge_sql = f"""
        MERGE `{target_table}` T
        USING `{staging_table}` S
        ON T.article_id = S.article_id
        WHEN NOT MATCHED THEN
          INSERT (ingested_at, article_id, title, description, url, published_at, source_name, _source)
          VALUES (S.ingested_at, S.article_id, S.title, S.description, S.url, S.published_at, S.source_name, S._source)
    """
    result = client.query(merge_sql).result()
    return result.num_dml_affected_rows or 0


@functions_framework.http
def handler(request: Any) -> tuple[dict[str, Any], int]:
    """HTTP entry point for the news extractor Cloud Function."""
    project = os.environ.get("BQ_PROJECT", "")
    dataset = os.environ.get("BQ_DATASET", "raw")
    api_key = os.environ.get("NEWS_API_KEY", "")

    if not api_key:
        return {"status": "error", "message": "NEWS_API_KEY not set"}, 500

    try:
        logger.info("Starting news extraction")
        records = _extract(api_key)
        inserted = _load_to_bq(records, project, dataset)
        logger.info("News load complete — rows_inserted=%d", inserted)
        return {"status": "ok", "rows_inserted": inserted}, 200
    except Exception as exc:
        logger.exception("News extraction failed: %s", exc)
        return {"status": "error", "message": str(exc)}, 500
