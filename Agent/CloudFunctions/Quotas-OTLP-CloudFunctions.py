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
OTLP_ENDPOINT = "http://localhost:4318/v1/metrics"
exporter = OTLPMetricExporter(endpoint=OTLP_ENDPOINT, timeout=10)

resource = Resource(attributes={"service.name": "gcp-quota-exporter"})
reader = PeriodicExportingMetricReader(exporter, export_interval_millis=5000)
meter_provider = MeterProvider(resource=resource, metric_readers=[reader])
metrics.set_meter_provider(meter_provider)

meter = metrics.get_meter("QuotaMetrics")

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
    """Reads project configs (ID, interval, metrics) from a CSV file."""
    try:
        df = pd.read_csv(file_path)
        required_cols = ['project_id', 'interval', 'metrics_to_check']
        if not all(col in df.columns for col in required_cols):
            raise ValueError(f"CSV file must have the following columns: {required_cols}")
        return df.to_dict('records')
    except FileNotFoundError:
        print(f"Error: Configuration file not found at '{file_path}'")
        return []

def get_quota_current_usage(project_id, metric_type):
    """Fetches the current usage for a given metric type."""
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
    """Fetches the current limits for all quotas."""
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
    """Combines usage and limit data."""
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
            # Only add limit if it matches a requested usage type
            if (quota_type == 'allocation' and allocation_usage_data) or \
               (quota_type == 'rate' and rate_usage_data):
                combined[key] = {"usage": None, "limit": limit["limit"], "type": quota_type}
    return combined

def update_otlp_metrics(combined_data):
    """Updates OTLP metrics with the combined quota data."""
    if not combined_data:
        return
        
    for key, data in combined_data.items():
        quota_metric, region, project, quota_type = key
        labels = {
            "quota_metric": quota_metric or "N/A",
            "region": region or "global",
            "project": project or "N/A",
            "type": quota_type or "N/A"
        }
        current_usage_counter.add(data.get("usage", 0), attributes=labels)
        quota_limit_counter.add(data.get("limit", -1), attributes=labels)

def fetch_and_process_project(config):
    """Fetches and processes specified quota data for a single project."""
    project_id = config['project_id']
    # Normalize and split the metrics to check
    metrics_to_check = {m.strip().lower() for m in config['metrics_to_check'].split(',')}
    
    print(f"Fetching metrics for project: {project_id} (checking: {list(metrics_to_check)})")
    
    try:
        allocation_usage, rate_usage = [], []
        # Conditionally fetch data based on the config
        if 'allocation' in metrics_to_check:
            allocation_usage = get_quota_current_usage(project_id, "serviceruntime.googleapis.com/quota/allocation/usage")
        if 'rate' in metrics_to_check:
            rate_usage = get_quota_current_usage(project_id, "serviceruntime.googleapis.com/quota/rate/net_usage")
        
        # Only fetch limits if we're checking at least one usage type
        if allocation_usage or rate_usage:
            limit_data = get_quota_current_limit(project_id)
            combined_data = combine_usage_and_limit(allocation_usage, rate_usage, limit_data)
            update_otlp_metrics(combined_data)
            print(f"Successfully processed project: {project_id}")
        else:
            print(f"No valid metrics specified for project: {project_id}. Skipping.")

    except Exception as e:
        print(f"An error occurred while processing project {project_id}: {e}")

# --- Main Execution ---

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="GCP Quota Exporter to OTLP")
    parser.add_argument(
        '--config',
        type=str,
        default='projects.csv',
        help='Path to the CSV config file. Default: projects.csv'
    )
    args = parser.parse_args()

    project_configs = read_project_configs_from_csv(args.config)
    
    if not project_configs:
        print("No project configurations to monitor. Exiting.")
        exit()

    # Initialize a dictionary to track the last time each project was checked
    last_checked = {config['project_id']: 0 for config in project_configs}

    print(f"Monitoring {len(project_configs)} projects with individual schedules and metrics.")
    
    try:
        while True:
            now = time.time()
            for config in project_configs:
                project_id = config['project_id']
                interval = config['interval']
                
                # Check if it's time to process this project
                if now >= last_checked.get(project_id, 0) + interval:
                    fetch_and_process_project(config)
                    last_checked[project_id] = now
            
            time.sleep(1) # Prevents a high-CPU busy-wait loop

    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        meter_provider.shutdown()
        print("OTLP Meter Provider has been shut down.")
