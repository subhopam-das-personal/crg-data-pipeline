"""
hello_lineage.py — Marquez connectivity validation.

Run this BEFORE building anything else:
  python hello_lineage.py

Expects Marquez running at OPENLINEAGE_URL (default: http://localhost:5000).
If you see "✅ Marquez is healthy" and "✅ RunEvent emitted", you're good to go.
"""
import os
import sys
import uuid
from datetime import datetime, timezone

import requests
from openlineage.client import OpenLineageClient
from openlineage.client.run import RunEvent, RunState, Run, Job, Dataset

OPENLINEAGE_URL = os.getenv("OPENLINEAGE_URL", "http://localhost:5000")
MARQUEZ_UI_URL = os.getenv("MARQUEZ_URL", "http://localhost:3000")


def check_health() -> bool:
    try:
        r = requests.get(f"{OPENLINEAGE_URL}/api/v1/namespaces", timeout=5)
        if r.status_code == 200:
            print(f"✅ Marquez is healthy at {OPENLINEAGE_URL}")
            return True
        print(f"❌ Marquez returned HTTP {r.status_code}")
        return False
    except Exception as e:
        print(f"❌ Cannot reach Marquez at {OPENLINEAGE_URL}: {e}")
        print("   → Make sure Marquez is running: docker-compose up marquez")
        return False


def emit_test_event() -> bool:
    try:
        client = OpenLineageClient(url=OPENLINEAGE_URL)
        event = RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=datetime.now(timezone.utc).isoformat(),
            run=Run(runId=str(uuid.uuid4())),
            job=Job(namespace="clinical-lineage-demo", name="hello_lineage_test"),
            producer="hello_lineage.py",
            inputs=[Dataset(namespace="clinical-lineage-demo", name="test_input")],
            outputs=[Dataset(namespace="clinical-lineage-demo", name="test_output")],
        )
        client.emit(event)
        print(f"✅ RunEvent emitted — check Marquez UI at {MARQUEZ_UI_URL}")
        print(f"   Navigate to: Jobs → hello_lineage_test")
        return True
    except Exception as e:
        print(f"❌ Failed to emit RunEvent: {e}")
        return False


if __name__ == "__main__":
    print(f"Checking Marquez at {OPENLINEAGE_URL} ...\n")
    healthy = check_health()
    if not healthy:
        sys.exit(1)
    emitted = emit_test_event()
    if not emitted:
        sys.exit(1)
    print("\n✅ All good. Start building.")
