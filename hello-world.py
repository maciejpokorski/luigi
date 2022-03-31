import luigi
import zipfile
import boto3
import botocore

from luigi.contrib.s3 import S3Client

class S3Download(luigi.ExternalTask):
    
    zip_path = "/mnt/c/Others/python/luigi-demo/download.zip"
    
    def output(self):
        return luigi.LocalTarget(self.zip_path)

    def run(self):
        return S3Client().get("s3://elasticbeanstalk-us-east-2-269645966560/employees.zip", self.zip_path)

class UnZip(luigi.Task):

    def requires(self):
        return S3Download()

    def output(self):
        return luigi.LocalTarget("employees.csv")

    def run(self):
        with zipfile.ZipFile(self.input().path) as zip_ref:
            zip_ref.extractall("./")
