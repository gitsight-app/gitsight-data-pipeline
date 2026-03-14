# Gitsight

## Getting Started

> To get started with Gitsight, follow these steps:

```shell
docker compose -f ./docker-compose-local.yaml up -d --build
```

### Airflow Connections

#### aws default

- AWS Access Key ID
- AWS Secret Access Key

#### catalog_default (Nessie Catalog)

- Extra Field (JSON):

```json
{
  "spark.sql.catalog.nessie": "org.apache.iceberg.spark.SparkCatalog",
  "spark.sql.catalog.nessie.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
  "spark.sql.catalog.nessie.uri": "https://{endPoint}:19120/api/v1",
  "spark.sql.catalog.nessie.ref": "main",
  "spark.sql.catalog.nessie.warehouse": "s3a://{warehouse_path}"
}
```

#### spark_default

- Host: spark://spark-master
- Port: 7077

# About Gitsight

## Skills

### Data Processing

<div>
<img src="https://img.shields.io/badge/apachespark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" alt=""/>
<img src="https://img.shields.io/badge/apacheairflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white" alt=""/>
<img src="https://img.shields.io/badge/python-3776AB?style=for-the-badge&logo=python&logoColor=white" alt=""/>
</div>

### Data Storage

<div>
<img src="https://img.shields.io/badge/apache_iceberg-50ABF1?style=for-the-badge&logoColor=white" alt=""/>
<img src="https://img.shields.io/badge/nessie_catalog-23D96C?style=for-the-badge&logoColor=white" alt=""/>
<img src="https://img.shields.io/badge/minio-C72E49?style=for-the-badge&logo=minio&logoColor=white" alt=""/>
<img src="https://img.shields.io/badge/apacheparquet-50ABF1?style=for-the-badge&logo=apacheparquet&logoColor=white" alt=""/>
<img src="https://img.shields.io/badge/postgresql-4169E1?style=for-the-badge&logo=postgresql&logoColor=white" alt=""/>
</div>

### CI/CD Workflow

<div>
    <img src="https://img.shields.io/badge/githubactions-2088FF?style=for-the-badge&logo=githubactions&logoColor=white" alt=""/>
    <img src="https://img.shields.io/badge/docker-2496ED?style=for-the-badge&logo=docker&logoColor=white" alt=""/>
</div>

### Visualization & Server

<div>
    <img src="https://img.shields.io/badge/react-61DAFB?style=for-the-badge&logo=react&logoColor=white" alt=""/>
    <img src="https://img.shields.io/badge/shadcnui-000000?style=for-the-badge&logo=shadcnui&logoColor=white" alt=""/>
    <img src="https://img.shields.io/badge/supabase-3FCF8E?style=for-the-badge&logo=supabase&logoColor=white" alt=""/>
</div>

### Data Flow

```mermaid
graph LR
    classDef bronze_t fill: #d7ccc8, stroke: #5d4037, stroke-width: 2px, color: black;
    classDef silver_t fill: #e0e0e0, stroke: #424242, stroke-width: 2px, color: black;
    classDef silver_v fill: #e0e0e0, stroke: #424242, stroke-width: 2px, color: black;
    classDef gold_t fill: #fff9c4, stroke: #fbc02d, stroke-width: 3px, color: black;
    classDef api fill: #e1f5fe, stroke: #01579b, stroke-width: 1px, color: black;


%% -------------------------- Components -------------------------- %%
    GH_ARCHIVE_API[(GHArchive API)]:::api
    GITHUB_OPEN_API[(GitHub OPEN API)]:::api
    GH_ARCHIVE[(raw/gharchive/yyyy-MM-dd-HH.json.gz)]
    GH_ARCHIVE_EVENTS[(bronze/gharchive_events/ingested_at_hour=yyyy-MM-dd-HH)]:::bronze_t
    ACTOR_META[(bronze/actor_meta/ingested_at_hour=yyyy-MM-dd-HH)]:::bronze_t
    REPO_META[(bronze/repo_meta/ingested_at_hour=yyyy-MM-dd-HH)]:::bronze_t
    GH_ARCHIVE_API --> GH_ARCHIVE
    GH_ARCHIVE -- overwritePartitions --> GH_ARCHIVE_EVENTS
    subgraph hourly: gharchive_events_ingest_dag
        GH_ARCHIVE_EVENTS -- append --> REPO_META
        GH_ARCHIVE_EVENTS -- append --> ACTOR_META

    end
%% --- %%
    ACTOR_MASTER[(silver/actor_master)]:::silver_t
    REPO_MASTER[(silver/repo_master)]:::silver_t

    subgraph hourly: repo_actor_master_transform_dag
        REPO_META -- dedup & upsert --> REPO_MASTER
        ACTOR_META -- dedup & upsert --> ACTOR_MASTER

    end
%% --- %%
    WATCH_EVENTS[(silver/watch_events/ingested_at_hour=yyyy-MM-dd-HH)]:::silver_t
    PR_EVENTS[(silver/pr_events/ingested_at_hour=yyyy-MM-dd-HH)]:::silver_t
    ISSUES_EVENTS[(silver/watch_events/ingested_at_hour=yyyy-MM-dd-HH)]:::silver_t
    PUSH_EVENTS[(silver/watch_events/ingested_at_hour=yyyy-MM-dd-HH)]:::silver_t
    FORK_EVENTS[(silver/watch_events/ingested_at_hour=yyyy-MM-dd-HH)]:::silver_t
    UNIFIED_EVENTS[Unified Events View]:::silver_v

    subgraph hourly: github_events_transform_dag
        GH_ARCHIVE_EVENTS -- overwritePartitions --> WATCH_EVENTS --> UNIFIED_EVENTS
        GH_ARCHIVE_EVENTS -- overwritePartitions --> PR_EVENTS --> UNIFIED_EVENTS
        GH_ARCHIVE_EVENTS -- overwritePartitions --> ISSUES_EVENTS --> UNIFIED_EVENTS
        GH_ARCHIVE_EVENTS -- overwritePartitions --> PUSH_EVENTS --> UNIFIED_EVENTS
        GH_ARCHIVE_EVENTS -- overwritePartitions --> FORK_EVENTS --> UNIFIED_EVENTS


    end
%% --- %%
    REPO_METRICS_HOURLY[(gold/repo_hourly_metrics/ingested_at_hour=yyyy-MM-dd-HH)]:::gold_t
    UNIFIED_EVENTS -- filter Star, Fork Events --> FILTERED_EVENTS

    subgraph hourly: repo_metrics_hourly_dag
        FILTERED_EVENTS -- overwritePartitions --> REPO_METRICS_HOURLY
    end
%% --- %%
    REPO_METRICS_DAILY[(gold/repo_hourly_metrics/created_at=yyyy-MM-dd)]:::gold_t

    subgraph daily: repo_metrics_daily_dag
        UNIFIED_EVENTS --> REPO_METRICS_DAILY


    end
%% --- %%
    ACTOR_DETAIL_RAW[(bronze/actor_detail_raw/ingested_at_hour=yyyy-MM-dd-HH)]:::bronze_t
    ACTOR_DETAIL_SCD[(silver/actor_detail_scd/is_current=true, false /ingested_at_hour=yyyy-MM-dd-HH/)]:::silver_t

    subgraph hourly: update_dim_actor_scd_dag
        UNIFIED_EVENTS --> ACTOR_DETAIL_RAW
        GITHUB_OPEN_API -- use mapPartitions, UDF --> ACTOR_DETAIL_RAW
        ACTOR_DETAIL_RAW -- dedup & upsert --> ACTOR_DETAIL_SCD


    end







```



