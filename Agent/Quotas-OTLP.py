#!/usr/bin/env python
# coding: utf-8

# In[1]:


from google.cloud import monitoring_v3
import time
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.resources import Resource
from google.auth import default


# OTLP Endpoint (LOCAL)
OTLP_ENDPOINT = "http://localhost:4318/v1/metrics"
exporter = OTLPMetricExporter(endpoint=OTLP_ENDPOINT, timeout=10)



# In[ ]:


# Configure the OpenTelemetry MeterProvider with OTLP exporter
resource = Resource(attributes={"service.name": "gcp-quota-exporter"})
reader = PeriodicExportingMetricReader(exporter, export_interval_millis=5000)  # Export every 5 seconds
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

def get_quota_current_usage(project_id, metric_type):
    """
    Fetch the current usage for a given metric type (allocation or rate).
    """
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
            quota_metric = metric_labels.get("quota_metric")
            resource_labels = time_series.resource.labels
            location = resource_labels.get("location")
            project_id = resource_labels.get("project_id")
            quotas.append({
                "quota_metric": quota_metric,
                "location": location,
                "project_id": project_id,
                "usage": value,
                "type": "allocation" if "allocation" in metric_type else "rate"
            })
    return quotas

def get_quota_current_limit(project_id):
    """
    Fetch the current limits for all quotas (both allocation and rate).
    """
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
            quota_metric = metric_labels.get("quota_metric")
            resource_labels = time_series.resource.labels
            location = resource_labels.get("location")
            project_id = resource_labels.get("project_id")
            quotas.append({
                "quota_metric": quota_metric,
                "location": location,
                "project_id": project_id,
                "limit": value,
            })
    return quotas

def combine_usage_and_limit(allocation_usage_data, rate_usage_data, limit_data):
    """
    Combine usage and limit data for both allocation and rate quotas.
    """
    combined = {}
    for usage in allocation_usage_data:
        key = (usage["quota_metric"], usage["location"], usage["project_id"], usage["type"])
        combined[key] = {"usage": usage["usage"], "limit": None, "type": usage["type"]}

    for usage in rate_usage_data:
        key = (usage["quota_metric"], usage["location"], usage["project_id"], usage["type"])
        combined[key] = {"usage": usage["usage"], "limit": None, "type": usage["type"]}

    for limit in limit_data:
        quota_type = "rate" if "rate" in limit["quota_metric"] else "allocation"
        key = (limit["quota_metric"], limit["location"], limit["project_id"], quota_type)
        if key in combined:
            combined[key]["limit"] = limit["limit"]
        else:
            combined[key] = {"usage": None, "limit": limit["limit"], "type": quota_type}
    return combined

def update_otlp_metrics(combined_data):
    """
    Update OTLP metrics with the combined quota data.
    """
    for key, data in combined_data.items():
        quota_metric, region, project, quota_type = key
        usage = data["usage"]
        limit = data["limit"]

        labels = {
            "quota_metric": quota_metric,
            "region": region,
            "project": project,
            "type": quota_type
        }

        # Record usage (default to 0 if None)
        current_usage_counter.add(usage if usage is not None else 0, attributes=labels)
        
        # Record limit (default to -1 if None to indicate no limit available)
        quota_limit_counter.add(limit if limit is not None else -1, attributes=labels)

def fetch_and_update_metrics(project_id):
    """
    Fetch quota data and update OTLP metrics periodically.
    """
    while True:
        allocation_usage_data = get_quota_current_usage(project_id, "serviceruntime.googleapis.com/quota/allocation/usage")
        rate_usage_data = get_quota_current_usage(project_id, "serviceruntime.googleapis.com/quota/rate/net_usage")
        limit_data = get_quota_current_limit(project_id)
        combined_data = combine_usage_and_limit(allocation_usage_data, rate_usage_data, limit_data)
        update_otlp_metrics(combined_data)
        print("Metrics pushed to OTLP endpoint")
        time.sleep(5)  # Update every 5 minutes (adjust as needed)

if __name__ == "__main__":
    project_id = "deep-learning-1984"  # Replace with your project ID
    try:
        fetch_and_update_metrics(project_id)
    except KeyboardInterrupt:
        print("Shutting down...")
        meter_provider.shutdown()


# In[ ]:





# In[ ]:




