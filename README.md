# Azure Olympic Data Engineering Pipeline

This project is an end-to-end data engineering pipeline built on Azure using the Tokyo Olympics dataset.
The goal was to understand how different Azure services work together in a real-world data pipeline.

---

## What this project does

* Ingests raw CSV data using Azure Data Factory
* Stores data in Azure Data Lake Gen2 (raw layer)
* Processes and transforms data using PySpark in Databricks
* Writes transformed data back to ADLS (transformed layer)
* Performs basic analysis using Azure Synapse

---

## Tech stack

* Azure Data Factory
* Azure Data Lake Gen2
* Azure Databricks (PySpark)
* Azure Synapse Analytics
* Python

---

## Data used

The dataset contains Olympic-related data:

* athletes.csv
* coaches.csv
* entriesgender.csv
* medals.csv
* teams.csv

---

## How the pipeline works

1. Data is uploaded into ADLS under `raw-data`
2. Databricks reads the data using ABFS paths
3. Transformations are applied using PySpark
4. Results are written to `transformed-data` folder
5. Synapse is used to query and analyze the final data

---

## Transformations performed

* Schema inference and structured reading
* Sorting countries based on gold medals
* Calculating gender participation ratios
* Basic data cleaning and formatting

---

## Sample insights

* United States has the highest number of gold medals
* Some sports have balanced gender participation, while others are skewed
* Athletics has one of the highest participation counts

---

## Project structure

```bash
azure-olympic-data-engineering-pipeline/

├── Code/              # Databricks notebooks / scripts
├── Datasets/          # Input data (CSV files)
├── Screenshots/       # Architecture and output screenshots
├── README.md
```

---

## How to run

1. Upload dataset to ADLS (raw-data folder)
2. Configure access in Databricks (OAuth / access key)
3. Run the Databricks notebook
4. Check transformed data in ADLS
5. Run queries in Synapse

---

## Notes

* Credentials are not included in this repository
* You will need your own Azure setup to run this project

---

## What I learned

* How to build an end-to-end data pipeline in Azure
* Working with ADLS using ABFS paths
* Using PySpark for transformations
* Integrating multiple Azure services together

---

## Future improvements

* Implement medallion architecture (Bronze/Silver/Gold)
* Add automation using ADF triggers
* Use Delta Lake for better performance
* Connect Power BI for visualization

---
