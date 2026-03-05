# NBA Data Ingestion Service

This repository contains an example implementation of a **DataUpdater** service
for collecting and warehousing NBA statistics.  The goal of this project is to
demonstrate best practices for building a modern data lakehouse pipeline
leveraging open data sources (e.g., [`nba_api`](https://github.com/swar/nba_api),
[`pbpstats`](https://www.pbpstats.com/)), scraped content (Basketball
Reference and Bball‑Index), and AWS services (S3, Glue, Iceberg, ECS, Step
Functions and EventBridge).  

End result is a set of Bronze, Silver and Gold tables that can be queried via Redshift Serverless or Spark.

## Should I use external spreadsheets for play‑by‑play (PbP) data?

The large spreadsheet schema provided in the question (fields such as
`game_id`, `period`, `away_score`, `home_score`, `event_type`, etc.) matches
the typical structure of a play‑by‑play feed.  


While these spreadsheets offer a convenient offline snapshot, the ingestion pipeline described here
already fetches play‑by‑play data via `pbpstats` and the official NBA API, which ensures up‑to‑date information with proper attribution.  Using unofficial spreadsheets can be helpful for quick prototyping or as a secondary source of truth, but they are not required for the core pipeline and may become outdated over time.  When combining multiple sources, carefully deduplicate records and trust the authoritative provider (pbpstats/nba_api) whenever possible.

## Repository Layout

```text
nba_ingestion_repo/
├── src/
│   └── data_updater.py       # Main orchestrator for ingestion
│   └── nba_ingestion/        # Package with modular client and scraper code
├── Dockerfile               # Builds a minimal container for running the updater
├── requirements.txt         # Python dependencies for the updater
├── docker-compose.yml       # Compose file for EC2 deployments
├── task_definition.json     # Sample ECS task definition with placeholders
├── .gitlab-ci.yml           # GitLab pipeline for building/deploying the service
├── infra/
│   └── template.yaml        # CloudFormation/SAM template for AWS resources
├── README.md                # This file
└── .gitignore               # Common exclusions
```

## Why use immutable image tags (`$CI_COMMIT_SHA`) instead of `latest`?

Reusing a tag like `latest` can overwrite an existing image with new code.
Best practices from AWS and the broader container community recommend tagging every build with a unique, immutable identifier (such as the Git commit SHA) and configuring repositories to prevent tag overwrites.  By using `$CI_COMMIT_SHA` as your tag:

* You **know exactly** which commit is running in production.
* Each deployment produces a new ECS task definition revision linked to a
  specific image, making rollbacks deterministic.
* You avoid race conditions where multiple pipeline runs could overwrite
  `latest` at the same time, causing unpredictable behavior.

Articles from AWS and community authors explain that `latest` is a
mutable label that “doesn’t convey what changes have been made” and
should be avoided in production; instead, use unique tags and enable
image tag immutability

## Deploying with GitLab CI/CD

The provided `.gitlab-ci.yml` defines a three‑stage pipeline: **test**,
**build** and **deploy**.  Build jobs use the Git commit SHA as the image tag
and push it to Amazon ECR.  Deploy jobs patch an ECS task definition with
this exact image tag and update the ECS service.  For EC2 targets, the
pipeline invokes AWS Systems Manager Run Command to perform a rolling
container update on the instance.  The pipeline assumes that the
following CI/CD variables are defined:

| Variable             | Description                                           |
|----------------------|-------------------------------------------------------|
| `ROLE_ARN`           | IAM role ARN that GitLab runners assume via OIDC    |
| `AWS_ACCOUNT_ID`     | Your AWS account ID                                  |
| `AWS_DEFAULT_REGION` | AWS region for ECR and ECS (e.g., `us-east-1`)       |
| `ECR_REPOSITORY`     | Name of the ECR repository for the DataUpdater       |
| `DEPLOY_TARGET`      | `ecs` or `ec2` to select the deployment path         |
| `ECS_CLUSTER`        | Name of the ECS cluster (for ECS deployments)        |
| `ECS_SERVICE`        | Name of the ECS service to update                    |
| `ECS_CONTAINER_NAME` | Name of the container in the task definition         |
| `EC2_INSTANCE_ID`    | EC2 instance ID (for EC2 deployments)                |
| `EC2_APP_DIR`        | Directory on the EC2 instance for docker‑compose      |

The `nba_ingestion` package contains several modules:

* `config.py` – Loads environment variables into a `Settings` dataclass.  Critical
  variables like `BRONZE_BUCKET` must be defined or the service will fail fast
  at import time.  Optional variables such as `BBALL_EMAIL` and
  `BBALL_PSWRD` enable scraping of Bball‑Index.
* `nba_api_client.py` – Thin wrapper around the `nba_api` library for fetching
  games, players and teams.  Handles missing dependencies gracefully.
* `pbpstats_client.py` – Stub for interacting with the `pbpstats` package.  You
  should extend this class with methods to pull the specific play‑by‑play and
  possession statistics listed in your requirements (e.g., seconds per
  possession, assisted vs. unassisted shots).
* `bball_index_scraper.py` – Handles authentication and scraping of
  player profiles from bball‑index.com using the credentials provided via
  `BBALL_EMAIL` and `BBALL_PSWRD`.  It uses `requests` and `BeautifulSoup4`
  under the hood and exposes methods to fetch individual profiles or
  multiple profiles in bulk.  If no credentials are supplied, the scraper
  step is skipped.

When adding new data sources or transformations, create a new module under
`nba_ingestion/` and instantiate it from `data_updater.py`.  This
architecture isolates syntax and import errors to individual modules,
making debugging easier during development.

For details on configuring OIDC, tag immutability and ECS releases, refer
to the AWS best practices for container deployments and the Docker tagging guidelines

## CloudFormation/SAM Template

The `infra/template.yaml` file is a minimal AWS CloudFormation/SAM
template that provisions the core infrastructure needed for this
pipeline:

* S3 buckets for Bronze, Silver and Gold layers with versioning and
  encryption enabled.
* An AWS Glue database and a sample Iceberg table registration (for the
  Silver layer).
* IAM roles for the ECS task and Step Functions with least privilege.
* An EventBridge Scheduler rule that triggers an ECS task periodically.
* A Step Functions state machine orchestrating ingestion and transformation
  (simplified for demonstration).

You will need to adjust bucket names, ARNs and scheduling cron expressions
before deploying the stack.  Use AWS SAM CLI (`sam deploy`) or the AWS
Console to create the stack.

## Running Locally or on EC2

For local development or EC2 deployments, use the provided
`docker-compose.yml`.  Set environment variables for S3 bucket names and
authentication, then run:

```bash
docker compose up -d updater
```

When running on EC2, the GitLab pipeline uses AWS Systems Manager
Run Command to pull the latest image and restart the container without
requiring SSH access.  This follows the AWS recommendation to avoid
directly logging into servers during CI/CD and leverages SSM’s
output‑capturing capabilities

## License

This project is provided as example code and carries no warranty.  Use
it at your own risk and adapt it to your organization’s requirements.