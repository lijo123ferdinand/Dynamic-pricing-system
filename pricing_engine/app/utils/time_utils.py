from datetime import datetime, timezone

def utcnow():
    return datetime.now(timezone.utc)

def utcnow_str() -> str:
    return utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
