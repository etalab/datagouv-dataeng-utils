import setuptools

with open("README.md", "r", encoding = "utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name = "datagouv_dataeng_utils",
    version = "0.0.3",
    author = "Geoffrey Aldebert",
    author_email = "geoffrey.aldebert@data.gouv.fr",
    description = "Utils for data engineering team in data.gouv.fr",
    long_description = long_description,
    long_description_content_type = "text/markdown",
    url = "https://github.com/etalab/datagouv-dataeng-utils",
    project_urls = {
        "Bug Tracker": "https://github.com/etalab/datagouv-dataeng-utils/issues",
    },
    classifiers = [
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    packages = setuptools.find_packages(),
    python_requires = ">=3.6",
        install_requires = [
        'requests >=2.27.1',
        'minio >= 7.1.1',
        'psycopg2 >= 2.9.1'
    ],
)