# Quotas-OTLP

This project contains two Python scripts for handling quota management, each with a different approach.

## Files

*   **Quotas-OTLP.py:** This is the main Python program for handling quota. It contains the core logic for managing and processing quota-related tasks.
*   **CloudFunctions/Quotas-OTLP-CloudFunctions.py:** This file contains a Google Cloud Function that also handles quota, but it's designed to be deployed and run in a serverless environment. It offers a different method of quota handling, potentially suited for event-driven or scalable scenarios.

## Description

The project provides two different ways to manage quotas. `Quotas-OTLP.py` is a standalone application, while `Quotas-OTLP-CloudFunctions.py` is a cloud function designed for a serverless architecture.

## Usage

*   To run the main Python program:
```
bash
    python Quotas-OTLP.py
    
```
* To manage `Quotas-OTLP-CloudFunctions.py` refer to the Google Cloud Function documentation.

## Requirements

*   Python 3.x