from setuptools import setup

setup(
    name="ecom-common",
    version="1.0.0",
    packages=["ecom_common"],
    install_requires=["prometheus-client>=0.20", "grpcio>=1.62"],
)
