# 🗽 NYC Trips Analytics

Every day, thousands of taxi rides crisscross New York City, generating a massive trail of untapped data. This project analyzes taxi trip data from 2024 to uncover how ride patterns shift across boroughs, how earnings are split between drivers and platforms, and how rider behavior changes over time. It helps answer key questions like:

- Which vendors have the highest trip volume growth over time?
- How does rider demand vary by weekday?
- Which boroughs generate the most trips and earnings?
- How much of each fare goes to drivers vs platforms?

## ⚙️ Tools Used

| Layer            | Technology     |
|------------------|----------------|
| Storage          | Google Cloud Storage (GCS) |
| Data Warehouse   | BigQuery       |
| Transformation   | DBT            |
| Visualization    | Looker Studio  |

## 🏗️ Data Architecture

This project follows the **Medallion Architecture** (Bronze → Silver → Gold) for building a scalable and maintainable data pipeline, and uses the **Star Schema** design pattern in the Gold layer for analytics.

1. **Bronze Layer – Raw Ingestion**  
   2024 NYC taxi trip datasets (Yellow, Green, FHV, HVFHV) are manually downloaded and stored in **Google Cloud Storage (GCS)**. External tables are defined in **BigQuery** to reference the raw CSVs without immediate data loading.

2. **Silver Layer – Cleansed and Enriched**  
   Using **DBT**, raw data is transformed into clean, structured models. This includes:  
   - Adding calculated fields (trip duration, fare breakdown)
   - Adding data quality flags

3. **Gold Layer – Aggregated Business Views**  
   Final DBT models are organized using a **Star Schema**, with:  
   - A central **fact table** for trips and earnings  
   - **Dimension tables** for date, location, payment, ratecode and vendor 
![Dimension Model](screenshots/dimensional%20modelling.jpeg)

4. **Visualization**  
   The Gold layer is connected to **Looker Studio** to visualize: 
   - Trends in trip volume and earnings  
   - Weekly rider behavior  
   - Vendor performance over time  
   - Borough-level distributions
![Dashboard](screenshots/dashboard.jpg)
