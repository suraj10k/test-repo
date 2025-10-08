#!/usr/bin/env python3
import os
import sys
import json
import time
import argparse
import requests
import boto3
from collections import OrderedDict
from typing import Dict, List, Tuple, Set, Optional
from botocore.exceptions import BotoCoreError, ClientError
from datetime import datetime, timedelta, timezone
 
LOG_INTERVAL_SECONDS = 60
LOOKBACK_WINDOW_MINUTES = 5
DEFAULT_STAT = "Maximum"
DEFAULT_PERIOD = 60
 
def validate_metric(i: int, m: dict) -> None:
    required_keys = {"label", "namespace", "metric_name", "dimensions", "acceptable_bounds"}
    missing = required_keys - set(m.keys())
    if missing:
        raise ValueError(f"Metric #{i} missing keys: {', '.join(sorted(missing))}")
 
    # dimensions
    dims = m["dimensions"]
    if not isinstance(dims, list) or not all(
        isinstance(d, dict) and {"Name", "Value"} <= set(d.keys()) for d in dims
    ):
        raise ValueError(f"Metric #{i} 'dimensions' must be a list of objects with 'Name' and 'Value'.")
 
    # bounds
    ab = m["acceptable_bounds"]
    if not isinstance(ab, dict) or not {"lower", "upper"} <= set(ab.keys()):
        raise ValueError(f"Metric #{i} 'acceptable_bounds' must contain 'lower' and 'upper'.")
    lower, upper = ab["lower"], ab["upper"]
    if not isinstance(lower, (int, float)) or not isinstance(upper, (int, float)):
        raise ValueError(f"Metric #{i} acceptable_bounds.lower/upper must be numbers.")
    if lower > upper:
        raise ValueError(f"Metric #{i} acceptable_bounds.lower cannot be greater than upper.")
 
    # optional diff threshold
    if "diff" in m:
        diff_val = m["diff"]
        if not isinstance(diff_val, (int, float)) or diff_val < 0:
            raise ValueError(f"Metric #{i} 'diff' must be a non-negative number.")
 
    # optional title
    if "title" in m and not (isinstance(m["title"], str) and m["title"].strip()):
        raise ValueError(f"Metric #{i} 'title' must be a non-empty string if provided.")
 
    # [{email,name}]
    if "mentions" in m:
        mentions = m["mentions"]
        if not isinstance(mentions, dict):
            raise ValueError(f"Metric #{i} 'mentions' must be an object.")
        if "label" in mentions:
            raise ValueError(f"Metric #{i} 'mentions.label' is not supported. Use 'mentions.title' only.")
        if "title" in mentions:
            arr = mentions["title"]
            if not isinstance(arr, list):
                raise ValueError(f"Metric #{i} 'mentions.title' must be an array.")
            for j, elt in enumerate(arr):
                if not isinstance(elt, dict):
                    raise ValueError(f"Metric #{i} 'mentions.title[{j}]' must be an object with 'email' and 'name'.")
                email = elt.get("email")
                name = elt.get("name")
                if not (isinstance(email, str) and "@" in email):
                    raise ValueError(f"Metric #{i} 'mentions.title[{j}].email' must be a valid email/UPN.")
                if not (isinstance(name, str) and name.strip()):
                    raise ValueError(f"Metric #{i} 'mentions.title[{j}].name' must be a non-empty string.")
 
def load_config(script_dir: str) -> List[dict]:
    path = os.path.join(script_dir, "metrics.json")
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Required metrics file not found: {path}\n"
        )
 
    with open(path, "r", encoding="utf-8") as f:
        root = json.load(f)
 
    if not isinstance(root, dict):
        raise ValueError('metrics.json root must be an OBJECT: { "metrics": [...] }')
 
    if "metrics" not in root or not isinstance(root["metrics"], list):
        raise ValueError('metrics.json must include "metrics" as an array.')
 
    metrics: List[dict] = root["metrics"]
 
    for i, m in enumerate(metrics):
        if not isinstance(m, dict):
            raise ValueError(f"Metric #{i} must be a JSON object.")
        validate_metric(i, m)
 
    return metrics
 
 
def extract_title_mentions(arr: List[dict]) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for elt in arr or []:
        email = str(elt["email"]).strip()
        name = str(elt["name"]).strip()
        mapping[email] = name
    return mapping
 
 
def build_watchers_by_title(metrics: List[dict]) -> Dict[str, Dict[str, str]]:
    watchers: Dict[str, Dict[str, str]] = {}
    for m in metrics:
        title = m.get("title", "General")
        arr = (m.get("mentions", {}) or {}).get("title", [])
        emails_map = extract_title_mentions(arr)
        if emails_map:
            if title not in watchers:
                watchers[title] = {}
            watchers[title].update(emails_map)
    return watchers

def build_metric_queries(metrics: List[dict]) -> Tuple[List[dict], Dict[str, str]]:
    queries = []
    id_map = {}
    for idx, m in enumerate(metrics):
        qid = f"m{idx}"
        period = m.get("period", DEFAULT_PERIOD)
        stat = m.get("stat", DEFAULT_STAT)
        queries.append({
            "Id": qid,
            "MetricStat": {
                "Metric": {
                    "Namespace": m["namespace"],
                    "MetricName": m["metric_name"],
                    "Dimensions": m["dimensions"],
                },
                "Period": period,
                "Stat": stat,
            },
            "ReturnData": True,
        })
        id_map[qid] = m["label"]
    return queries, id_map
 
 
def fetch_latest_metrics(cloudwatch, metrics: List[dict]) -> Dict[str, Tuple[Optional[datetime], Optional[float]]]:
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(minutes=LOOKBACK_WINDOW_MINUTES)
    queries, id_map = build_metric_queries(metrics)
    resp = cloudwatch.get_metric_data(
        MetricDataQueries=queries,
        StartTime=start_time,
        EndTime=end_time,
        ScanBy="TimestampAscending",
    )
    out: Dict[str, Tuple[Optional[datetime], Optional[float]]] = {}
    for r in resp.get("MetricDataResults", []):
        label = id_map.get(r.get("Id"))
        ts_list = r.get("Timestamps", []) or []
        val_list = r.get("Values", []) or []
        if not ts_list or not val_list:
            out[label] = (None, None)
            continue
        pairs = sorted(zip(ts_list, val_list))
        latest_ts, latest_val = pairs[-1]
        if latest_ts.tzinfo is None:
            latest_ts = latest_ts.replace(tzinfo=timezone.utc)
        out[label] = (latest_ts, latest_val)
 
    for m in metrics:
        out.setdefault(m["label"], (None, None))
    return out
 
def group_by_title(metrics: List[dict]) -> "OrderedDict[str, List[dict]]":
    grouped: "OrderedDict[str, List[dict]]" = OrderedDict()
    for m in metrics:
        title = m.get("title", "General")
        grouped.setdefault(title, []).append(m)
    return grouped
 
def build_mentions_entities(email_to_name: Dict[str, str]) -> Tuple[str, List[dict]]:
    if not email_to_name:
        return "", []
 
    at_fragments: List[str] = []
    entities: List[dict] = []
 
    for email, name in sorted(email_to_name.items(), key=lambda kv: (kv[1].lower(), kv[0].lower())):
        display = name or email
        at_text = f"<at>{display}</at>"
        at_fragments.append(at_text)
        entities.append({
            "type": "mention",
            "text": at_text,               
            "mentioned": {"id": email, "name": display}
        })
 
    return "Notifying: " + " ".join(at_fragments), entities
 
 
def build_adaptive_card(grouped_lines: List[str], email_to_name: Dict[str, str]) -> dict:
    body: List[dict] = []
    for line in grouped_lines:
        if line and not line.startswith("  - "):
            body.append({
                "type": "TextBlock", "text": line,
                "weight": "Bolder", "size": "Medium", "wrap": True, "spacing": "Medium"
            })
        else:
            body.append({
                "type": "TextBlock", "text": line, "wrap": True, "spacing": "Small"
            })
 
    at_line, entities = build_mentions_entities(email_to_name)
    if at_line:
        body.append({"type": "TextBlock", "text": at_line, "wrap": True, "spacing": "Medium"})
 
    card_content = {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.4",
        "body": body,
        "msteams": {"entities": entities}
    }
 
    return {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": card_content
            }
        ]
    }
 
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Poll CloudWatch metrics (from metrics.json) and post grouped status to Microsoft Teams."
    )
    p.add_argument("--aws-profile", required=True, help="AWS named profile for credentials (e.g., default)")
    p.add_argument("--aws-region", required=True, help="AWS region for CloudWatch metrics (e.g., ap-northeast-1)")
    p.add_argument("--webhook-url", required=True, help="Microsoft Teams Webhook URL (Workflows or Incoming Webhook)")
    return p.parse_args()
 
def main() -> None:
    args = parse_args()
 
    script_dir = os.path.dirname(os.path.abspath(__file__))
 
    try:
        metrics = load_config(script_dir)
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        sys.exit(1)
 
    print(f"Using profile: {args.aws_profile}, region: {args.aws_region}")
    print(f"Polling every {LOG_INTERVAL_SECONDS}s; default PERIOD={DEFAULT_PERIOD}s, default STAT={DEFAULT_STAT}")
 
    grouped_cfg = group_by_title(metrics)
    print("Tracking metrics (grouped):")
    for title, group in grouped_cfg.items():
        print(f"  {title}")
        for m in group:
            diff_info = f", diff_threshold={m['diff']}" if "diff" in m else ""
            print(
                f"    - {m['label']} :: {m['namespace']}/{m['metric_name']} "
                f"(stat={m.get('stat', DEFAULT_STAT)}, period={m.get('period', DEFAULT_PERIOD)}) "
                f"acceptable_bounds=[{m['acceptable_bounds']['lower']}, {m['acceptable_bounds']['upper']}]{diff_info}"
            )
    print("Press Ctrl+C to stop.\n")
 
    session = boto3.Session(profile_name=args.aws_profile, region_name=args.aws_region)
    cloudwatch = session.client("cloudwatch")
 
    # Store previous values for diff comparison
    previous_values: Dict[str, Optional[float]] = {m["label"]: None for m in metrics}
 
    # main loop
    try:
        while True:
            now = datetime.now(timezone.utc)
            try:
                latest = fetch_latest_metrics(cloudwatch, metrics)
 
                # Precompute title-level watchers (email->name) from inline mentions
                watchers_by_title = build_watchers_by_title(metrics)
 
                lines: List[str] = []
                mentions_email_to_name: Dict[str, str] = {}
                titles_with_oob: Set[str] = set()
 
                for title, group in group_by_title(metrics).items():
                    lines.append(title)
                    for m in group:
                        label = m["label"]
                        lb = m["acceptable_bounds"]["lower"]
                        ub = m["acceptable_bounds"]["upper"]
                        diff_threshold = m.get("diff")
                        ts, val = latest.get(label, (None, None))
                        prev_val = previous_values.get(label)
 
                        if ts is None or val is None:
                            lines.append(f"  - {label} = NA")
                        else:
                            # Check absolute bounds violation
                            bounds_violated = not (lb <= val <= ub)
                            
                            # Check diff threshold violation
                            diff_violated = False
                            diff_delta = None
                            if diff_threshold is not None and prev_val is not None:
                                diff_delta = val - prev_val
                                diff_violated = diff_delta >= diff_threshold
                            
                            # Determine status
                            if bounds_violated or diff_violated:
                                suffix = ""
                                if diff_violated and diff_delta is not None:
                                    suffix = f" (Δ+{round(diff_delta, 3)})"
                                lines.append(f"  - {label} ‼️ = {round(val, 3)}{suffix}")
                                titles_with_oob.add(title)
                            else:
                                lines.append(f"  - {label} ✅ = {round(val, 3)}")
                            
                            # Update previous value for next iteration
                            previous_values[label] = val
                    
                    lines.append("")
 
                # Add title-level watchers ONCE per title that had any OOB metric
                for t in titles_with_oob:
                    for email, name in watchers_by_title.get(t, {}).items():
                        mentions_email_to_name[email] = name
 
                # Build and POST Adaptive Card
                card_payload = build_adaptive_card(
                    grouped_lines=[l for l in lines if l.strip() != ""],
                    email_to_name=mentions_email_to_name
                )
 
                resp = requests.post(
                    args.webhook_url,
                    headers={"Content-Type": "application/json"},
                    data=json.dumps(card_payload),
                    timeout=15
                )
                if resp.status_code // 100 != 2:
                    raise SystemExit(f"Teams webhook failed: {resp.status_code} | {resp.text[:500]}")
                print(f"{now.isoformat()}Z Posted Adaptive Card. HTTP {resp.status_code}")
 
            except (ClientError, BotoCoreError) as e:
                print(f"{now.isoformat()}Z, error=\"{type(e).__name__}: {e}\"")
 
            time.sleep(LOG_INTERVAL_SECONDS)
 
    except KeyboardInterrupt:
        print("\nStopping…")
 
if __name__ == "__main__":
    main()
