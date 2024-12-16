# AG Challenge

This project automates the generation, validation, and upload of mock data for Users, Transactions, and User Preferences into BigQuery using Airflow.

---

## Project Overview

1. Generating mock data based on the provided schema.
2. Automating tasks with Airflow to:
   - Generate data.
   - Upload CSV files into BigQuery.
   - Validate the uploaded data.
   - Test email notifications via SMTP.

3. Airflow DAG runs tasks sequentially:
   - `generate_data`: Generate mock data files.
   - `upload_into_bigquery`: Load data into BigQuery tables.
   - `validate_bigquery_data`: Validate data has been uploaded correctly.
   - `test_email_configuration`: Test email functionality to notify about failures.

---

## Prerequisites
1. Python 3.8+.
2. Airflow installed and configured.
3. BigQuery access configured with the `bq` CLI tool.
4. SMTP access enabled for sending notifications (e.g., Outlook SMTP).

---

## Dependencies

```bash
pip install -r requirements.txt