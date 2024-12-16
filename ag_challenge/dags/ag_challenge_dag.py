from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email_smtp
from datetime import datetime, timedelta
from dotenv import load_dotenv
import subprocess
import logging
import os


# load env. variables
load_dotenv()

# env. variables
EMAIL_RECIPIENT = os.getenv("EMAIL_RECIPIENT")
if not EMAIL_RECIPIENT:
    raise ValueError("EMAIL_RECIPIENT environment variable is not set.")

# enhanced logging config.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "email_on_failure": True,
    "email": [EMAIL_RECIPIENT],
}

# sending a custom failure email using send_email_smtp
def send_failure_email(context):
    subject = f"Airflow Task Failed: {context['task_instance_key_str']}"
    body = f"""
    DAG: {context['task_instance'].dag_id}
    Task: {context['task_instance'].task_id}
    Execution Time: {context['execution_date']}
    Log URL: {context['task_instance'].log_url}
    """
    send_email_smtp(to=EMAIL_RECIPIENT, subject=subject, html_content=body)
    logging.info("Failure email sent successfully.")

# testing SMTP email config
def send_test_email():
    try:
        send_email_smtp(to=EMAIL_RECIPIENT, subject="Airflow Test Email", html_content="This is a test email :)")
        logging.info("Email test sent successfully.")
    except Exception as e:
        logging.error(f"Sending test email failed: {e}")
        raise

# DAG defined
with DAG(
    dag_id="ag_challenge_etl",
    default_args=default_args,
    description="Generate data, upload to BigQuery, validate, and test email",
    schedule="@daily",
    start_date=datetime(2024, 6, 10),
    catchup=False,
    on_failure_callback=send_failure_email,
) as dag:

    # Task 1: generate data
    def run_data_scripts():
        scripts = [
            "python importers/transactions/generate_transactions.py",
            "python importers/user_preferences/generate_preferences.py",
            "python importers/users/generate_users.py",
        ]
        for script in scripts:
            try:
                logging.info(f"Running script: {script}")
                subprocess.run(script, shell=True, check=True)
            except subprocess.CalledProcessError as e:
                logging.error(f"Script {script} failed: {e}")
                raise

    task_generate_data = PythonOperator(
        task_id="generate_data",
        python_callable=run_data_scripts,
        sla=timedelta(minutes=5),
    )

    # Task 2: upload CSV files into BigQuery
    def upload_into_bigquery():
        bigquery_commands = [
            "bq load --source_format=CSV --autodetect ag_challenge_data.users data/structure/users_data.csv",
            "bq load --source_format=CSV --autodetect ag_challenge_data.user_preferences data/structure/user_preferences_data.csv",
            "bq load --source_format=CSV --autodetect ag_challenge_data.transactions data/structure/transactions_data.csv",
        ]
        for command in bigquery_commands:
            try:
                logging.info(f"Running BigQuery command: {command}")
                subprocess.run(command, shell=True, check=True)
            except subprocess.CalledProcessError as e:
                logging.error(f"BigQuery load failed: {e}")
                raise

    task_upload_into_bigquery = PythonOperator(
        task_id="upload_into_bigquery",
        python_callable=upload_into_bigquery,
    )

    # Task 3: validate data
    def validate_bigquery_data():
        tables = {
            "users": "SELECT COUNT(*) AS row_count FROM ag_challenge_data.users",
            "user_preferences": "SELECT COUNT(*) AS row_count FROM ag_challenge_data.user_preferences",
            "transactions": "SELECT COUNT(*) AS row_count FROM ag_challenge_data.transactions",
        }
        for table_name, query in tables.items():
            try:
                logging.info(f"Validating table {table_name}")
                subprocess.run(f"bq query --use_legacy_sql=false '{query}'", shell=True, check=True)
            except subprocess.CalledProcessError as e:
                logging.error(f"Validation failed for table {table_name}: {e}")
                raise

    task_validate_data = PythonOperator(
        task_id="validate_bigquery_data",
        python_callable=validate_bigquery_data,
    )

    # Task 4: test email config.
    task_test_email = PythonOperator(
        task_id="test_email_configuration",
        python_callable=send_test_email,
    )

    # define task dependencies
    task_generate_data >> task_upload_into_bigquery >> task_validate_data >> task_test_email

if __name__ == "__main__":
    dag.test()