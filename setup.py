# setup.py
from setuptools import setup, find_packages

setup(
    name='my_airflow_project',
    version='0.2',
    packages=find_packages(include=["lib*", "scripts*"]),
)