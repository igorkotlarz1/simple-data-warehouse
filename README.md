# SQL Data Warehouse Project: Integrating CRM & ERP Data

## A complete Data Warehouse developed entirely in Microsoft SQL Server (T-SQL). 
> The main focus of this project was to build an ETL Pipeline and integrate raw business data from independent **CRM (Customer Relationship Management)** and **ERP (Enterprise Resource Planning)** sources into a **Single Source of Truth**, utilizing data cleansing and transformation techniques according to the **Medallion architecture** pattern (**Bronze, Silver, Gold** Layers). Finally, the data was modelled into a **Star-Schema**, ready for future Business Intelligence analytics.


## Data Architecture 
The project follows the Medallion Architecture Pattern:
* **Bronze layer** - Ingests raw CSV data from local storage. It is used as the staging area for the extracted data, leaving the exact structure of source files.
* **Silver layer** - Performs data cleansing, deduplication, standardization and anomaly resolution. 
* **Gold layer** - Business-ready data modelled into a **Star Schema** using views. 

## Technical Highlights
Here are some key technical implementations:
* **Raw Data Ingestion -** Used `BULK INSERT` statements to mass load the CSV data into Bronze Layer tables.
* **Data Quality Management -** Handled data issues such as missing values, negative prices, data inconsistencies across multiple tables and finally data standardization into user-friendky format (gender, product categories) using `CASE` logic. 
* **Utilizing SQL Window Functions:**
   * Applied `ROW_NUMBER()` to deduplicate customer records based on the account creation dates.
   * Used `LEAD()` to fix and generate accurate product end dates for historical tracking.  
* **Dimensional Modeling** (in the Gold Layer):
  * Designed the Gold Layer as **Star Schema**, where `sales` act the *fact table* surrounded by `customer` and `product` *dimensions* providing additional business context.
  * Generated **Surrogate Keys** to decouple the Data Warehouse from source system primary keys.
* **ETL Orchestration-** Built a master procedure (`run_pipeline`) to automate the execution of the Bronze and Silver loads, additionally logging the execution times for each layer.
---
## Project Structure:

 ```text
simple-data-warehouse/
|
├── data/                          # Raw CSV datasets
| 
├── docs/                          # Project documentation
|   ├── data_integration.png       # Data integration diagram (relations between the tables)
|   ├── data_model.png             # Data model (Star Schema) diagram
|
├── sql_scripts/                   # SQL scripts 
|    ├── init_db.sql               # Script for database initialization
|    ├── run_pipeline.sql          # Master ETL Pipeline procedure
|    ├── bronze/                   # DDL and load procedures for Bronze Layer
|    ├── silver/                   # DDL and load procedures for Silver Layer
|    ├── gold/                     # Star Schema modeling (views)
 ```

## Acknowledgements
This project was inspired by the data engineering content created by **Data with Baraa** on YouTube.
It utilizes his provided CSV datasets and core Medallion Architecture concepts.
* Check out his channel - [Data with Baraa](https://www.youtube.com/@DataWithBaraa) and his [Data Warehouse tutorial](https://www.youtube.com/watch?v=9GVqKuTVANE)
* Check out his [GitHub repo](https://github.com/DataWithBaraa/sql-data-warehouse-project) for the original solution and source datasets
