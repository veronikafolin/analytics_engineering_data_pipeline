from dagfactory import DagFactory

dag_factory = DagFactory("/home/airflow/gcs/dags/dag_factory_version/flat_version/dashboard_factory/config.yml")

dag_factory.clean_dags(globals())
dag_factory.generate_dags(globals())
