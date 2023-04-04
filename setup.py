from setuptools import setup, find_packages

setup(
    name='nhanes_utils',
    version='1.0.0',
    description='A package that can be used to download and process data from the NHANES.',
    author='Toby Rea',
    author_email='toby.e.rea@protonmail.com',
    url='https://github.com/toby-rea/nhanes-utils',
    license='MIT',
    packages=find_packages(),
    install_requires=[
        'numpy',
        'pandas',
        'pyreadstat',
        'requests',
        'selectolax',
    ],
)
