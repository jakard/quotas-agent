#!/usr/bin/env python
# coding: utf-8

import time
import argparse
import pandas as pd
from google.cloud import monitoring_v3
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.resources import Resource
from google.auth import default

# --- Configuration ---
# OTLP Endpoint (LOCAL)
OTLP_ENDPOINT = "http://localhost:4318/v1/metrics"
exporter = OTLPMetricExporter(endpoint=OTLP_ENDPOINT, timeout=10)

# Configure the OpenTelemetry MeterProvider with OTLP exporter
resource = Resource(attributes={"service.name": "gcp-quota-exporter"})
reader = PeriodicExportingMetricReader(exporter, export_interval_millis=5000)
meter_provider = MeterProvider(resource=resource, metric_readers=[reader])
metrics.set_meter_provider(meter_provider)

# Create a meter
meter = metrics.get_meter("QuotaMetrics")

# Define counters for usage and limits
current_usage_counter = meter.create_counter(
    name="gcp_quota_current_usage",
    description="Current usage of GCP quotas",
    unit="1"
)
quota_limit_counter = meter.create_counter(
    name="gcp_quota_limit",
    description="Quota limit of GCP quotas",
    unit="1"
)

# --- Functions ---

def read_project_configs_from_csv(file_path):
    """Reads project configurations (ID and interval) from a CSV file."""
    try:
        df = pd.read_csv(file_path)
        if 'project_id' not in df.columns or 'interval' not in df.columns:
            raise ValueError("CSV file must have 'project_id' and 'interval' columns.")
        # Convert the DataFrame to a list of dictionaries
        return df.to_dict('records')
    except FileNotFoundError:
        print(f"Error: Configuration file not found at '{file_path}'")
        return []

def get_quota_current_usage(project_id, metric_type):
    """Fetch the current usage for a given metric type (allocation or rate)."""
    client = monitoring_v3.MetricServiceClient()
    end_time = time.time()
    start_time = end_time - 50000
    response = client.list_time_series(
        request={
            "name": f"projects/{project_id}",
            "filter": f'metric.type = "{metric_type}"',
            "interval": {
                "end_time": {"seconds": int(end_time)},
                "start_time": {"seconds": int(start_time)},
            },
        }
    )
    quotas = []
    for time_series in response:
        for point in time_series.points:
            value = point.value.int64_value
            metric_labels = time_series.metric.labels
            resource_labels = time_series.resource.labels
            quotas.append({
                "quota_metric": metric_labels.get("quota_metric"),
                "location": resource_labels.get("location"),
                "project_id": resource_labels.get("project_id"),
                "usage": value,
                "type": "allocation" if "allocation" in metric_type else "rate"
            })
    return quotas

def get_quota_current_limit(project_id):
    """Fetch the current limits for all quotas (both allocation and rate)."""
    client = monitoring_v3.MetricServiceClient()
    metric_type = "serviceruntime.googleapis.com/quota/limit"
    end_time = time.time()
    start_time = end_time - 86400  # Last 24 hours
    response = client.list_time_series(
        request={
            "name": f"projects/{project_id}",
            "filter": f'metric.type = "{metric_type}"',
            "interval": {
                "end_time": {"seconds": int(end_time)},
                "start_time": {"seconds": int(start_time)},
            },
        }
    )
    quotas = []
    for time_series in response:
        for point in time_series.points:
            value = point.value.int64_value
            metric_labels = time_series.metric.labels
            resource_labels = time_series.resource.labels
            quotas.append({
                "quota_metric": metric_labels.get("quota_metric"),
                "location": resource_labels.get("location"),
                "project_id": resource_labels.get("project_id"),
                "limit": value,
            })
    return quotas

def combine_usage_and_limit(allocation_usage_data, rate_usage_data, limit_data):
    """Combine usage and limit data for both allocation and rate quotas."""
    combined = {}
    all_usage_data = allocation_usage_data + rate_usage_data

    for usage in all_usage_data:
        key = (usage["quota_metric"], usage["location"], usage["project_id"], usage["type"])
        combined[key] = {"usage": usage["usage"], "limit": None, "type": usage["type"]}

    for limit in limit_data:
        quota_type = "rate" if "/rate/" in limit["quota_metric"] else "allocation"
        key = (limit["quota_metric"], limit["location"], limit["project_id"], quota_type)
        if key in combined:
            combined[key]["limit"] = limit["limit"]
        else:
            combined[key] = {"usage": None, "limit": limit["limit"], "type": quota_type}
    return combined

def update_otlp_metrics(combined_data):
    """Update OTLP metrics with the combined quota data."""
    if not combined_data:
        return
        
    for key, data in combined_data.items():
        quota_metric, region, project, quota_type = key
        usage = data["usage"]
        limit = data["limit"]

        labels = {
            "quota_metric": quota_metric or "N/A",
            "region": region or "global",
            "project": project or "N/A",
            "type": quota_type or "N/A"
        }

        current_usage_counter.add(usage if usage is not None else 0, attributes=labels)
        quota_limit_counter.add(limit if limit is not None else -1, attributes=labels)

def fetch_and_process_project(project_id):
    """Fetches and processes quota data for a single project."""
    print(f"Fetching metrics for project: {project_id}...")
    try:
        allocation_usage = get_quota_current_usage(project_id, "serviceruntime.googleapis.com/quota/allocation/usage")
        rate_usage = get_quota_current_usage(project_id, "serviceruntime.googleapis.com/quota/rate/net_usage")
        limit_data = get_quota_current_limit(project_id)
        
        combined_data = combine_usage_and_limit(allocation_usage, rate_usage, limit_data)
        update_otlp_metrics(combined_data)
        
        print(f"Successfully processed project: {project_id}")
    except Exception as e:
        print(f"An error occurred while processing project {project_id}: {e}")

# --- Main Execution ---

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="GCP Quota Exporter to OTLP")
    parser.add_argument(
        '--config',
        type=str,
        default='projects.csv',
        help='Path to the CSV file containing project IDs and intervals. Default: projects.csv'
    )
    args = parser.parse_args()

    project_configs = read_project_configs_from_csv(args.config)
    
    if not project_configs:
        print("No project configurations to monitor. Exiting.")
        exit()

    # Create a dictionary to track the last time each project was checked
    # Initialize to 0 to ensure they all run on the first loop
    last_checked = {config['project_id']: 0 for config in project_configs}

    print(f"Monitoring {len(project_configs)} projects with individual intervals.")
    
    try:
        while True:
            now = time.time()
            for config in project_configs:
                project_id = config['project_id']
                interval = config['interval']
                
                # Check if it's time to process this project
                if now >= last_checked[project_id] + interval:
                    fetch_and_process_project(project_id)
                    # Update the last checked time for this project
                    last_checked[project_id] = now
            
            # Sleep for a short duration to prevent a busy-wait loop
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        meter_provider.shutdown()
        print("OTLP Meter Provider has been shut down.")
