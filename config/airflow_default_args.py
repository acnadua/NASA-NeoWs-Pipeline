from datetime import timedelta

default_args = {
    'owner': 'Abigail Nadua',
    'depends_on_past': False,
    'email': ['abinadua07@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}