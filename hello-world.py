import luigi
import zipfile
import boto3
import botocore
from sqlalchemy import create_engine
from luigi.contrib.s3 import S3Client
import pandas


class S3Download(luigi.ExternalTask):

    zip_path = "/mnt/c/Others/python/luigi-demo/download.zip"

    def output(self):
        return luigi.LocalTarget(self.zip_path)

    def run(self):
        return S3Client().get(
            "s3://elasticbeanstalk-us-east-2-269645966560/employees.zip",
            self.zip_path)


class UnZip(luigi.Task):

    def requires(self):
        return S3Download()

    def output(self):
        return luigi.LocalTarget("employees.csv")

    def run(self):
        with zipfile.ZipFile(self.input().path) as zip_ref:
            zip_ref.extractall("./")


class LoadCSVToDB(luigi.Task):
    username = 'root'
    password = 'root'
    host = 'localhost'
    port = 3306
    DB_NAME = 'employees'

    engine = create_engine(
        f"mysql://{username}:{password}@{host}:{port}/{DB_NAME}")

    def requires(self):
        return UnZip()

    def run(self):
        colnames = [
            'emp_no',
            'birth_date',
            'first_name',
            'last_name',
            'gender',
            'hire_date']
        data = pandas.read_csv(
            "employees.csv",
            names=colnames,
            parse_dates=[
                'birth_date',
                'hire_date'])
        data.to_sql('employees', self.engine, index=False, if_exists='append')

    def complete(self):
        query = "SELECT * FROM employees WHERE emp_no >= 500000"
        with self.engine.connect() as conn:
            results = conn.execute(query).fetchall()
            return len(results) == 3


class MasterTask(luigi.WrapperTask):
    def requires(self):
        yield S3Download()
        yield UnZip()
        yield LoadCSVToDB()
