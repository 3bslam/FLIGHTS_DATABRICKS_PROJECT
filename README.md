# FLIGHTS_DATABRICKS_PROJECT
end to end data engineer project with databricks
Project Goal and Architecture:
◦ I created this project to learn the latest Databricks technologies, including real-world scenarios.
◦ In this project, I followed the Medallion Architecture ✨, a popular structure in Data Engineering. I divided the data into three main layers:

Bronze (Raw Data) 🥉

Silver (Cleaned and Transformed Data) 🥈

Gold (Analytics-ready Data Models) 🥇

• Bronze Layer (Data Ingestion):
◦ Source: I worked with flight data files (CSV) that arrive daily and require incremental processing.
◦ Tools: I used Databricks Autoloader, built on Spark Structured Streaming, to load the data incrementally.
![Screenshot 2025-07-06 205648](https://github.com/user-attachments/assets/28d429ac-e277-419a-9b1e-bdc923498d44)


I didn’t build static solutions; instead, I created dynamic solutions using parameter analysis and Databricks Jobs workflow control.

Autoloader ensures exactly-once processing and automatically tracks schema evolution.

This was implemented using Databricks Free Edition 🆓, with Unity Catalog and Volumes, eliminating the need to manage external cloud storage.


• Silver Layer (Data Transformation):
◦ Tools: I leveraged the latest Databricks technology: Lakehouse Declarative Pipelines (formerly Delta Live Tables - DLT), which is now part of open-source Apache Spark 🎉.
![Screenshot 2025-07-06 205945](https://github.com/user-attachments/assets/be9ed590-a6ee-47f8-a306-1707e95fe08f)


I adopted a declarative approach that simplifies data processing tasks instead of procedural logic.

Performed core transformations like data type conversions, added a modify_date column for tracking processing time, and removed unnecessary columns like rescue_data.

Applied data quality expectations 🚨 (e.g., no nulls allowed) with flexible options: warning, failure, or record dropping.

Used Auto CDC ♻️ (formerly known as apply changes) to perform upsert operations for changing data in dimension tables.
![Screenshot 2025-07-06 210042](https://github.com/user-attachments/assets/71bf8da1-947a-4f9f-b355-561e6202d6b3)


• Gold Layer (Dimensional Star Schema Modeling):
◦ Objective: In this layer, I aimed to build a Star Schema 🌟, including Fact Tables and Dimension Tables.
◦ Key Innovations:


Created a Dynamic Dimension Builder 🛠️ that can turn any source table into a dimension using just a few parameters. It supports both initial and incremental loads—demonstrating experienced data engineering skills.

Built a Dynamic Fact Builder 📈 that dynamically links dimension tables to a fact table, supporting upsert logic for incremental fact data.
◦ Technology: I used PySpark SQL queries to develop these dynamic and complex solutions.
![Screenshot 2025-07-06 210420](https://github.com/user-attachments/assets/95b96d9f-eab4-4017-b9fd-251c8bbe6214)


• Data Access & Consumption:
◦ After building the Gold Layer, I made the data accessible to users such as Data Analysts, Data Scientists, and BI Developers.
◦ I used Databricks SQL Warehouse 📊 (previously called Endpoints), a specialized compute type optimized for SQL workloads—ideal for reporting and analytics.
◦ These endpoints can be connected to tools like Power BI and Tableau 🔗 for dashboarding and analysis.
![Screenshot 2025-07-06 210615](https://github.com/user-attachments/assets/fdc814a7-0e0a-406e-8124-12d1d4fc09b5)


• Key Achievements of the Project:
◦ I built this project from scratch, which allowed me to gain deep knowledge and advanced insights.
◦ Focused on building dynamic, reusable solutions 🔄 that can be applied across different data warehouses.
◦ Utilized Databricks Free Edition 💰, enabling me to explore advanced features without cost.
◦ Covered real-world scenarios, making this project industry-grade and practical.

