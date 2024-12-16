# Sphere Entertainment Revenue Data Pipeline & Analytics

## Overview

This project simulates a data pipeline for Sphere Entertainment, specifically focusing on revenue data. It demonstrates the design, development, and implementation of an end-to-end ETL (Extract, Transform, Load) process to support data warehousing and analytics, as if it were being used by the Revenue Strategy and Optimization team. This project is designed to align with the responsibilities and skills outlined in the Data Engineer job description at Sphere Entertainment Co., and to showcase my ability to create a real-world solution using the tools and techniques they would use in a professional environment.

## Key Features

*   **Synthetic Revenue Data Generation:** A Python script (`data_generator.py`) that creates realistic, synthetic revenue data to mimic actual business transactions for various events, venues, and ticket types.
*   **Python-Based ETL Pipeline:** An ETL script (`etl_pipeline.py`) that extracts the synthetic data from a CSV file, transforms it by cleaning it and validating its integrity, and then loads it into an Amazon Redshift data warehouse.
*   **Amazon Redshift Data Warehouse:** Utilizes Amazon Redshift as the destination database to simulate a real-world cloud-based environment, showcasing my ability to work with relational cloud databases.
*   **Data Validation:** Implements data validation and cleaning during the ETL process (using `pandas`) and post data load with SQL queries to ensure data accuracy and integrity for governance, as well as automated notifications to warn users of any errors.
*   **Robust Logging:** The code is well-documented with logging to track the progress of each task in the pipeline and log any issues or errors.

## Technologies Used

*   **Python:** Core language for data generation, ETL logic, and database interaction.
*   **SQL:** For data loading, querying, and data validation in Redshift.
*   **Amazon Redshift:** Cloud-based data warehouse.
*   **Python Libraries:** `pandas`, `faker`, `psycopg2`, `sqlalchemy`, `python-dotenv`
*  **Version control:** `Git`, `Github`

## Alignment with Sphere Entertainment Data Engineer Role

This project was designed with the Sphere Entertainment Data Engineer role requirements in mind:

*   **Data Pipeline Design:** The ETL pipeline was designed to extract, transform, and load data into Redshift, and demonstrates my ability to design an end-to-end data pipeline.
*   **Python and SQL:** Python and SQL were used extensively in this project to manipulate the data, and to connect to the Redshift database.
*   **Cloud Data Warehouse:** Using Amazon Redshift demonstrates my ability to work in a cloud-based environment.
*   **Data Validation:** The implementation of data validation techniques shows my understanding of data accuracy and quality and its importance for data governance.
*  **Automation:** This project demonstrates my ability to create an automated data pipeline that is repeatable.

## Skills Demonstrated

This project showcases my proficiency in the following areas:

*   **Python Programming:** Including data manipulation with pandas, data generation with faker, and database connections with psycopg2.
*   **SQL Database Design and Querying:** Including database design, data loading, and data validation with SQL.
*  **ETL Processes** I have demonstrated my ability to design and implement an end-to-end ETL process.
*   **Amazon Redshift:** Ability to create a cloud-based data warehouse in Redshift.
*   **Data Validation and Quality:** Creating robust data validation logic with pandas and SQL.
* **Problem Solving and Debugging:** This project demonstrates my ability to solve problems, and to debug common issues.
*   **Collaboration:** Demonstrates my ability to collaborate on a project with others using Git and GitHub
