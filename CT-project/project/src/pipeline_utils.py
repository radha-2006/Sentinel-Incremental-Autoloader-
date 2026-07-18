import json
import logging
import os
from datetime import datetime

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger("ProjectSentinel")

def log_event(event_type, message, status="INFO", details=None):
    """
    Log an event as structured JSON for easy parsing by monitoring systems (e.g. Databricks logs, Log Analytics)
    """
    log_record = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "level": status,
        "event_type": event_type,
        "message": message,
        "pipeline": "ProjectSentinel"
    }
    if details:
        log_record["details"] = details
        
    logger.info(json.dumps(log_record))

def create_directory_structure(base_path):
    """
    Pre-creates directory layout for raw landing and checkpoint folders.
    """
    directories = [
        os.path.join(base_path, "raw_landing"),
        os.path.join(base_path, "bronze", "transactions"),
        os.path.join(base_path, "silver", "transactions"),
        os.path.join(base_path, "gold", "transactions"),
        os.path.join(base_path, "checkpoints", "bronze_chkpt"),
        os.path.join(base_path, "checkpoints", "bronze_schema"),
        os.path.join(base_path, "checkpoints", "silver_chkpt"),
        os.path.join(base_path, "checkpoints", "gold_chkpt"),
    ]
    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)
            log_event("DIRECTORY_CREATION", f"Created directory: {directory}", "DEBUG")
