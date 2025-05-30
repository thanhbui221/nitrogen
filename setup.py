from setuptools import setup, find_packages

setup(
    name="nitrogen",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "pyspark==3.5.0",
        "pyyaml==6.0.1",
        "python-dotenv==1.0.0"
    ]
) 