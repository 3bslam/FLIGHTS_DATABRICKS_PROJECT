# FLIGHTS_DATABRICKS_PROJECT
end to end data engineer project with databricks
Project Goal and Architecture:
◦ I created this project to master the latest Databricks technologies, including real-world scenarios.
◦ In this project, I followed the Medallion Architecture ✨, a popular structure in Data Engineering. I divided the data into three main layers:

Bronze (Raw Data) 🥉

Silver (Cleaned and Transformed Data) 🥈

Gold (Analytics-ready Data Models) 🥇
◦ This project serves as a valuable resource for learners as it covers real-world scenarios and provides deep knowledge and advanced insights.

• Bronze Layer (Data Ingestion):
◦ Source: I worked with flight data files (CSV) that arrive daily and require incremental processing.
◦ Tools: I used Databricks Autoloader, built on Spark Structured Streaming, to load the data incrementally.
◦ Highlights:


I didn’t build static solutions; instead, I created dynamic solutions using parameter analysis and Databricks Jobs workflow control.
![Screenshot 2025-07-06 210615](https://github.com/user-attachments/assets/f0b53e4d-2412-4455-8831-4b1dfea349d7)


Autoloader ensures exactly-once processing and automatically tracks schema evolution.

This was implemented using Databricks Free Edition 🆓, with Unity Catalog and Volumes, eliminating the need to manage external cloud storage.

• Silver Layer (Data Transformation):
◦ Tools: I leveraged the latest Databricks technology: Lakehouse Declarative Pipelines (formerly Delta Live Tables - DLT), which is now part of open-source Apache Spark 🎉.
◦ Highlights:
![Screenshot 2025-07-06 210420](https://github.com/user-attachments/assets/1e073b82-5142-45ad-94de-fec079c196cf)


I adopted a declarative approach that simplifies data processing tasks instead of procedural logic.

Performed core transformations like data type conversions, added a modify_date column for tracking processing time, and removed unnecessary columns like rescue_data.

Applied data quality expectations 🚨 (e.g., no nulls allowed) with flexible options: warning, failure, or record dropping.

Used Auto CDC ♻️ (formerly known as apply changes) to perform upsert operations for changing data in dimension tables.

• Gold Layer (Dimensional Star Schema Modeling):
◦ Objective: In this layer, I aimed to build a Star Schema 🌟, including Fact Tables and Dimension Tables.
◦ Key Innovations:
![Screenshot 2025-07-06 210218](https://github.com/user-attachments/assets/4fd75d3e-03e6-45f8-8178-e1e30a5f0582)

Created a Dynamic Dimension Builder 🛠️ that can turn any source table into a dimension using just a few parameters. It supports both initial and incremental loads—demonstrating experienced data engineering skills.

Built a Dynamic Fact Builder 📈 that dynamically links dimension tables to a fact table, supporting upsert logic for incremental fact data.
◦ Technology: I used PySpark SQL queries to develop these dynamic and complex solutions.
![Screenshot 2025-07-06 205945](https://github.com/user-attachments/assets/386f26c6-c609-4748-a358-22ce62cc8b84)


• Data Access & Consumption:
◦ After building the Gold Layer, I made the data accessible to users such as Data Analysts, Data Scientists, and BI Developers.
◦ I used Databricks SQL Warehouse 📊 (previously called Endpoints), a specialized compute type optimized for SQL workloads—ideal for reporting and analytics.
◦ These endpoints can be connected to tools like Power BI and Tableau 🔗 for dashboarding and analysis.

• Key Achievements of the Project:
◦ I built this project from scratch, which allowed me to gain deep knowledge and advanced insights.
◦ Focused on building dynamic, reusable solutions 🔄 that can be applied across different data warehouses.
◦ Utilized Databricks Free Edition 💰, enabling me to explore advanced features without cost.
◦ Covered real-world scenarios, making this project industry-grade and practical.![Screenshot 2025-07-06 205648](https://github.com/user-attachments/assets/8d887d48-3e3e-4adf-998a-8eb8111eda5c)
![Screenshot 2025-07-06 210042](https://github.com/user-attachments/assets/45fab0a6-262e-45bf-965f-cdf556fb5780)
![Screenshot 2025-07-06 210304](https://github.com/user-attachments/assets/e4a27aac-62d5-4699-95ae-04bea9457e99)
![Screenshot 2025-07-06 210346](https://github.com/user-attachments/assets/7ac2992b-eeae-488a-96f7-c039ed6f51d5)

