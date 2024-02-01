# Simulation of a Data Pipeline for Analytics Engineering

## Architecture 

<img src="docs/AEDP.png"/>

## Goal

The goal of the project was to design, implement, test and document a solution that uses the **Data Build Tool (DBT)** data transformation framework in combination with the **Snowflake** data management system in the **Google Cloud Platform** cloud environment.

In particular, the solution allows you to:

- Automatically manage data ingestion from Cloud Storage to Snowflake.
- Collect raw data on Snowflake in a format suitable for the transformation process.
- Transform, test and document data using the DBT tool. This process allows you to clean, normalize, enrich and prepare data for analysis and reporting.
- Monitor the transformation process using the Elementary package and configure Slack Alerts in case of errors.
- Collect transformed data on Snowflake.
- Orchestrate the transformation process using Cloud Composer and receive alerts (emails), via the SendGrid service, when the workflow fails.
- View and analyze the transformed data using the Looker Studio dashboarding tool, as well as monitor its data quality.
- Manage automatically via GitHub Actions:
  - synchronization between the project repository and the execution environment that orchestrates the transformation process.
  - the deployment of the documentation produced in a GitHub Pages.

## Artifacts

At this [link](https://veronikafolin.github.io/analytics_engineering_data_pipeline/) are available the project artifacts, in particular:

- _Project Docs_, a detailed explanation of the implementation, setup and use process of the project
- _DBT Docs_, which clarifies the transformation process implemented with DBT
- _Dashboards_, namely the reports created to display the data present in the data warehouse during the analysis phase.

## Author

Veronika Folin - veronika.folin@studio.unibo.it, v.folin@reply.it