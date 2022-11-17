# sconkie-cloud-composer
A data pipeline using Cloud Composer and other GCP tools.

[![GitHub Super-Linter](https://github.com/sean-conkie/sconkie-cloud-composer/workflows/Lint%20Code%20Base/badge.svg)](https://github.com/marketplace/actions/super-linter)
[![Validate DAG Config](https://github.com/sean-conkie/sconkie-cloud-composer/actions/workflows/validatedagconfig.yml/badge.svg)](https://github.com/sean-conkie/sconkie-cloud-composer/actions/workflows/validatedagconfig.yml)

## IAM
The following roles are required in GCP to allow Cloud Composer build and run.
### Service Account
Cloud Composer API Service Agent
Cloud Composer v2 API Service Agent Extension
Composer Worker
Service Account User
BigQuery Editor
BigQuery Job User

### Users
#### Editor
Service Account User
Composer Administrator
Environment and Storage Object Administrator

#### Viewer
Composer User
Environment and Storage Object Viewer


## Resources
### Airflow
[Airflow Scaling Workers](https://www.astronomer.io/guides/airflow-scaling-workers/) - key settings that you should consider modifying as you scale up your data pipelines.

[Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html)

[Cross-DAG Dependencies](https://airflow.apache.org/docs/apache-airflow/stable/howto/operator/external_task_sensor.html#cross-dag-dependencies)

### Cloud Composer
[Service Aaccount Roles](https://cloud.google.com/composer/docs/composer-2/access-control)

### Jinja Templating
[Jinja Tutorial](https://zetcode.com/python/jinja/) - From a blog, basic Jinja templating tutorial

[Jinja Docs](https://jinja.palletsprojects.com/en/3.1.x/)

## GitHub Actions
### super-linter

#### SQLFluff
[Rules](https://docs.sqlfluff.com/en/stable/rules.html)
