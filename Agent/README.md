# Quotas-OTLP

This project contains two Python scripts for handling quota management, each with a different approach.

## Files

*   **Quotas-OTLP.py:** This is the main Python program for handling quota. It contains the core logic for managing and processing quota-related tasks.
*   **CloudFunctions/Quotas-OTLP-CloudFunctions.py:** This file contains a Google Cloud Function that also handles quota, but it's designed to be deployed and run in a serverless environment. It offers a different method of quota handling, potentially suited for event-driven or scalable scenarios.

## Description

The project provides two different ways to manage quotas. `Quotas-OTLP.py` is a standalone application, while `Quotas-OTLP-CloudFunctions.py` is a cloud function designed for a serverless architecture.

## CSV

projects.csv example:

Code snippet

project_id,interval,metrics_to_check
project-alpha-12345,300,allocation,rate
project-beta-67890,900,allocation
project-gamma-11223,1800,rate

Explanation:

project-alpha will be checked for both allocation and rate quotas every 5 minutes.

project-beta will be checked for only allocation quotas every 15 minutes.

project-gamma will be checked for only rate quotas every 30 minutes.

## Usage

*   To run the main Python program:
```
bash
    python quota_exporter.py --config your_config_file.csv
    
```
* To manage `Quotas-OTLP-CloudFunctions.py` refer to the Google Cloud Function documentation.

## Requirements

*   Python 3.x
