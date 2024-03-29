from setuptools import setup, find_packages

setup(
    name='nhanes_utils',
    version='1.0.0',
    description='Utilities for working with NHANES data',
    author='Toby Rea',
    author_email='toby.e.rea@protonmail.com',
    url='https://github.com/toby-rea/nhanes_utils',
    license='MIT',
    packages=find_packages(),
    install_requires=[
        'pandas',
        'polars',
        'requests',
        'selectolax',
        'aiohttp',
        'aiofiles',
    ],
)
