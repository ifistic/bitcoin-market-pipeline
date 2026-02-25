###bitcoin_chain_ETL with dbt aws snowflake and dagster in Progress
# DU University Chapters ETL Pipeline  
### A Production-Ready Data Engineering Project

---

## Project Overview

This project implements a modular and scalable **ETL (Extract, Transform, Load) pipeline** built with **Python** and integrated with **Google Cloud Platform (GCP)** services.

It follows modern Data Engineering best practices including:

- Modular ETL architecture
- Structured logging
- Environment-based configuration
- Unit & integration testing
- Secure service account authentication
- BigQuery-ready loading layer
- Clean and maintainable project structure

---

#  ETL Architecture

![ETL Architecture](image/ETL.drawio.png)

---

## Architecture Flow

### Extract
- Pull data from Ducks Unlimited University Chapters API System
- Implemented in: `app/extractor.py`

### Transform
- Clean, validate, and standardize data
- Implemented in: `app/main.py`

### Load
- Load processed data into BigQuery
- Implemented in: `app/loader.py`

### Logging
- Centralized structured logging configuration
- Implemented in: `app/logging_config.py`

---

# 📂 Project Structure
du-university-chapters-etl/
│
├── app/                  # Core application (ETL logic)
├── image/                # Architecture diagrams / documentation assets
├── tests/                # Unit & integration tests
├── requirements.txt      # Project dependencies
└── readme.md             # Project documentation

