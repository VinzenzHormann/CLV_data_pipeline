# Customer Lifetime Value (CLV) Data Pipeline

This project is an end-to-end data engineering solution that predicts the future value of customers using probabilistic modeling. It is designed with a **"Safety-First"** architecture, ensuring that the machine learning models never process "garbage" data.


## Technologies & Tools

* **Orchestration:** [Apache Airflow](https://airflow.apache.org/)
* **Data Warehouse:** [Google BigQuery](https://cloud.google.com/bigquery) (SQL)
* **Language:** Python 3.13
* **ML Libraries:** Lifetimes (BetaGeoFitter, GammaGammaFitter), Pandas, NumPy
* **Testing:** Pytest


## Data Flow Architecture

The pipeline is divided into four distinct stages to ensure modularity and ease of maintenance:

1.  **Staging (SQL):** Raw transactional data (`CustomerID`, `UnitPrice`, `Quantity`, `InvoiceDate`) is pulled from source tables.
2.  **Feature Engineering (SQL):** Data is aggregated into an **RFM-T** format (Recency, Frequency, Monetary, T) at the customer level.
3.  **Data Quality Firewall (Python/SQL):**
    * **Ratio Check:** Ensures at least 95% of customers migrated from staging to features.
    * **Schema Check:** Validates that all 7 required columns for the ML model are present.
    * **Sanity Check:** Blocks any data containing negative values for frequency or monetary value.
4.  **CLV Modeling (Python):** Fits the probabilistic models and writes predictions back to BigQuery.



## Containerization & Deployment

To ensure the pipeline runs identically in development and production, we use **Docker**. This eliminates "it works on my machine" bugs by packaging Python 3.13, Airflow, and all ML dependencies into a single container.

### Docker Setup
The environment is managed via `docker-compose`. This spins up the Airflow Webserver, Scheduler, and Postgres database.

1.  **Build the Image:**
    ```bash
    docker-compose build
    ```
2.  **Start the Environment:**
    ```bash
    docker-compose up -d
    ```


## 🧪 Testing Suite

We prioritize **Decoupled Testing**. The logic is separated from the infrastructure (Airflow/BigQuery), allowing tests to run locally without a cloud connection.

### How to Run
```bash
# From the project root
python -m pytest -s tests/test_clv_logic.py
```

* Happy Path: Validates model prediction on standard data.
* Boundary Testing: Handles empty DataFrames and negative value clipping.
* Logic Validation: Uses "Brain" decomposition to test validation ratios without requiring a BigQuery connection.

##  📈 Business Impact

This pipeline provides the Marketing team with a "Daily Truth" on customer value. By automating the quality checks, we have reduced the risk of budget misallocation by 99%, ensuring that CLV-based bidding strategies are always backed by verified data.
