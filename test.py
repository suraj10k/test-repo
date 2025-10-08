#!/usr/bin/env python3
import os
import sys
import json
import time
import argparse
import requests
import boto3
from datetime import datetime, timedelta, timezone
from botocore.exceptions import BotoCoreError, ClientError

# Configuration constants
LOG_INTERVAL_SECONDS = 60
LOOKBACK_WINDOW_MINUTES = 5
DEFAULT_STAT = "Maximum"
DEFAULT_PERIOD = 60

def load_metrics(script_dir):
    """Load and validate metrics from metrics.json"""
    config_path = os.path.join(script_dir, "metrics.json")
    
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"metrics.json not found at: {config_path}")
    
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)
    
    if "metrics" not in config or not isinstance(config["metrics"], list):
        raise ValueError("metrics.json must contain a 'metrics' array")
    
    # Validate each metric
    for idx, metric in enumerate(config["metrics"]):
        required = ["label", "namespace", "metric_name", "dimensions", "acceptable_bounds"]
        missing = [k for k in required if k not in metric]
        if missing:
            raise ValueError(f"Metric #{idx} missing required fields: {missing}")
        
        # Validate bounds
        bounds = metric["acceptable_bounds"]
        if bounds["lower"] > bounds["upper"]:
            raise ValueError(f"Metric #{idx}: lower bound cannot exceed upper bound")
        
        # Validate diff if present
        if "diff" in metric and (not isinstance(metric["diff"], (int, float)) or metric["diff"] < 0):
            raise ValueError(f"Metric #{idx}: 'diff' must be a non-negative number")
    
    return config["metrics"]

def fetch_metrics(cloudwatch, metrics):
    """Fetch latest metric values from CloudWatch"""
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(minutes=LOOKBACK_WINDOW_MINUTES)
    
    # Build queries with unique IDs
    queries = []
    for idx, metric in enumerate(metrics):
        queries.append({
            "Id": f"m{idx}",
            "MetricStat": {
                "Metric": {
                    "Namespace": metric["namespace"],
                    "MetricName": metric["metric_name"],
                    "Dimensions": metric["dimensions"],
                },
                "Period": metric.get("period", DEFAULT_PERIOD),
                "Stat": metric.get("stat", DEFAULT_STAT),
            },
            "ReturnData": True,
        })
    
    try:
        response = cloudwatch.get_metric_data(
            MetricDataQueries=queries,
            StartTime=start_time,
            EndTime=end_time,
            ScanBy="TimestampAscending",
        )
    except (ClientError, BotoCoreError) as e:
        print(f"ERROR fetching metrics: {e}")
        return {}
    
    # Parse results and map back to metric indices
    results = {}
    for result in response.get("MetricDataResults", []):
        metric_id = result.get("Id")
        if not metric_id or not metric_id.startswith("m"):
            continue
        
        idx = int(metric_id[1:])  # Extract index from "m0", "m1", etc.
        timestamps = result.get("Timestamps", [])
        values = result.get("Values", [])
        
        if timestamps and values:
            # Get the latest value
            pairs = sorted(zip(timestamps, values))
            latest_ts, latest_val = pairs[-1]
            results[idx] = latest_val
        else:
            results[idx] = None
    
    return results

def build_teams_message(metrics, current_values, previous_values):
    """Build Microsoft Teams adaptive card with metric status"""
    grouped = {}  # Group by title
    alerts = {}   # Track which titles have alerts
    mentions = {} # Track who to mention
    
    for idx, metric in enumerate(metrics):
        title = metric.get("title", "General")
        label = metric["label"]
        current = current_values.get(idx)
        previous = previous_values.get(idx)
        
        if title not in grouped:
            grouped[title] = []
        
        # Determine metric status
        if current is None:
            status_line = f"  - {label} = NA"
        else:
            lower = metric["acceptable_bounds"]["lower"]
            upper = metric["acceptable_bounds"]["upper"]
            diff_threshold = metric.get("diff")
            
            # Check violations
            bounds_violated = not (lower <= current <= upper)
            diff_violated = False
            delta = None
            
            if diff_threshold and previous is not None:
                delta = current - previous
                diff_violated = delta >= diff_threshold
            
            # Build status message
            if bounds_violated or diff_violated:
                violations = []
                if bounds_violated:
                    violations.append("bounds")
                if diff_violated:
                    violations.append(f"Δ+{round(delta, 2)}")
                
                suffix = f" ({', '.join(violations)})" if violations else ""
                status_line = f"  - {label} ‼️ = {round(current, 2)}{suffix}"
                
                # Mark this title for alerts
                if title not in alerts:
                    alerts[title] = True
                
                # Extract mentions for this metric's title
                if "mentions" in metric and "title" in metric["mentions"]:
                    for person in metric["mentions"]["title"]:
                        email = person.get("email")
                        name = person.get("name")
                        if email and name:
                            mentions[email] = name
                
                print(f"[ALERT] {title}/{label}: current={round(current, 2)}, "
                      f"previous={previous}, bounds=[{lower}, {upper}], "
                      f"diff_threshold={diff_threshold}, delta={delta}")
            else:
                status_line = f"  - {label} ✅ = {round(current, 2)}"
        
        grouped[title].append(status_line)
    
    # Build message lines
    lines = []
    for title, status_lines in grouped.items():
        lines.append(title)
        lines.extend(status_lines)
        lines.append("")
    
    # Build adaptive card
    card_body = []
    for line in lines:
        if line and not line.startswith("  - "):
            # Title header
            card_body.append({
                "type": "TextBlock",
                "text": line,
                "weight": "Bolder",
                "size": "Medium",
                "wrap": True,
                "spacing": "Medium"
            })
        elif line:
            # Metric line
            card_body.append({
                "type": "TextBlock",
                "text": line,
                "wrap": True,
                "spacing": "Small"
            })
    
    # Add mentions if there are alerts
    entities = []
    if mentions:
        mention_text = "Notifying: " + " ".join([
            f"<at>{name}</at>" for email, name in sorted(mentions.items())
        ])
        card_body.append({
            "type": "TextBlock",
            "text": mention_text,
            "wrap": True,
            "spacing": "Medium"
        })
        
        entities = [
            {
                "type": "mention",
                "text": f"<at>{name}</at>",
                "mentioned": {"id": email, "name": name}
            }
            for email, name in mentions.items()
        ]
    
    return {
        "type": "message",
        "attachments": [{
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.4",
                "body": card_body,
                "msteams": {"entities": entities}
            }
        }]
    }

def main():
    parser = argparse.ArgumentParser(description="CloudWatch metrics monitor with Teams notifications")
    parser.add_argument("--aws-profile", required=True, help="AWS profile name")
    parser.add_argument("--aws-region", required=True, help="AWS region (e.g., ap-northeast-1)")
    parser.add_argument("--webhook-url", required=True, help="Microsoft Teams webhook URL")
    args = parser.parse_args()
    
    # Load configuration
    script_dir = os.path.dirname(os.path.abspath(__file__))
    try:
        metrics = load_metrics(script_dir)
    except Exception as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        sys.exit(1)
    
    print(f"Loaded {len(metrics)} metrics from metrics.json")
    print(f"AWS Profile: {args.aws_profile}, Region: {args.aws_region}")
    print(f"Polling interval: {LOG_INTERVAL_SECONDS}s\n")
    
    # Initialize AWS client
    session = boto3.Session(profile_name=args.aws_profile, region_name=args.aws_region)
    cloudwatch = session.client("cloudwatch")
    
    # Track previous values for diff calculation
    previous_values = {}
    
    print("Starting monitoring loop... (Press Ctrl+C to stop)\n")
    
    try:
        while True:
            timestamp = datetime.now(timezone.utc).isoformat()
            
            # Fetch current metrics
            current_values = fetch_metrics(cloudwatch, metrics)
            
            if not current_values:
                print(f"{timestamp} | Failed to fetch metrics, retrying...")
                time.sleep(LOG_INTERVAL_SECONDS)
                continue
            
            # Build and send Teams message
            message = build_teams_message(metrics, current_values, previous_values)
            
            try:
                response = requests.post(
                    args.webhook_url,
                    headers={"Content-Type": "application/json"},
                    json=message,
                    timeout=15
                )
                
                if response.status_code == 200:
                    print(f"{timestamp} | Posted update to Teams successfully")
                else:
                    print(f"{timestamp} | Teams webhook error: {response.status_code} - {response.text[:200]}")
            except requests.exceptions.RequestException as e:
                print(f"{timestamp} | Teams webhook request failed: {e}")
            
            # Update previous values for next iteration
            previous_values = current_values.copy()
            
            time.sleep(LOG_INTERVAL_SECONDS)
    
    except KeyboardInterrupt:
        print("\nStopping monitoring...")

if __name__ == "__main__":
    main()
