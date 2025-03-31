# Overview

Team Presentation: https://northcoders.com/project-phase/team-sidley

Welcome to Team Sidley's project! Our team—Kyle, Simon, Liam, Zamir, Ishmail, and Russell—has developed an application to handle Extract, Transform, and Load (ETL) processes. The system moves data from a prepared source into a data lake and finally into a warehouse hosted in AWS. Our goal was to create a fully automated pipeline capable of raising its own alerts if issues arise.

## Project Scope

The project simulates a commercial backend for a company that sells tote bags. The company requires an automated method to transfer data from its backend database into a data warehouse. This enables non-technical users, such as data scientists, to conduct analyses for business insights and growth.

## Technologies Used

- AWS Services: Lambda, S3, CloudWatch, Secrets Manager, IAM, EventBridge, SNS
- Terraform: Infrastructure as Code (IaC)
- Python: Used for scripting (ETL process)
- GitHub: CI/CD pipeline management
- PostgreSQL: Database system
- AWS QuickSight & Custom JavaScript Visualization Tool: Data visualization

## Agile Development Approach

We adopted an Agile Scrum methodology, using GitHub Projects as our Kanban board. Our workflow included:

- Story point estimation
- Epics grouping
- Sprint planning
- Daily stand-ups
- Pair programming
- Branch restrictions for quality control

## CI/CD Pipeline

- Our CI/CD pipeline ensures high-quality code through:
- Security vulnerability testing with safety and Bandit
- 90%+ test coverage enforcement
- Code compliance with PEP 8 using pylint
- Continuous deployment authenticated via GitHub Secrets
- Deployment of Terraform configurations to separate environments

## ETL Pipeline Design

1. Extract

- Uses Python with psycopg2 and boto3.
- Retrieves data from PostgreSQL and uploads it to S3 as JSON.
- Pulls only new rows since the last extraction.

2. Transform

- Utilizes pandas for schema normalization.
- Converts data into Parquet format.
- Ensures data integrity with deep copies.

3. Load

- Uploads transformed data to the warehouse.
- Uses io.BytesIO to store data in-memory before uploading.

## Test-Driven Development (TDD)

We followed a TDD approach, ensuring:

- Modular, reusable code
- Comprehensive unit testing with Moto (mock AWS services)
- Simulated PostgreSQL database testing
- Reliable Lambda execution testing

## Terraform Infrastructure as Code (IaC)

We structured Terraform files by service for maintainability. Key resources deployed:

- Lambda functions (with dependencies)
- S3 buckets (for data storage)
- CloudWatch (logging and monitoring)
- Secrets Manager (secure credential storage)
- IAM policies (permissions management)
- EventBridge (scheduling tasks)
- SNS (email notifications)

Data Visualization

- AWS QuickSight for business analytics.
- Custom JavaScript Dashboard with:
-- Drop-down toggle for real-time data filtering.
-- Interactive parameter selection.
-- Live database updates.

## Conclusion

Our project successfully meets the specified requirements and provides a robust, scalable ETL pipeline. 