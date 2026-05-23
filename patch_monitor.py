import re

with open("scripts/monitor.py", "r") as f:
    content = f.read()

def replacer(match):
    return """
def start_test(profile, dataset="synthetic"):
  global TEST_PROCESS
  with TEST_LOCK:
    if TEST_PROCESS is not None and TEST_PROCESS.poll() is None:
      return False, dict(TEST_STATE)
    TEST_LOG.parent.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    
    topics = 3 if dataset == "all" else 1
    if profile == "raw":
      max_events = 5
      expected = max_events * topics
      env.update({
          "DATASET": dataset,
          "MAX_EVENTS": str(max_events),
          "EXPECTED_MESSAGES": str(expected),
          "OBJECTS": "5",
          "QUERIES": "1",
          "K": "2",
      })
      command = [
          "bash", "-lc",
          "scripts/setup-venv.sh && "
          f"DATASET={dataset} MAX_EVENTS={max_events} EXPECTED_MESSAGES={expected} OBJECTS=5 QUERIES=1 DIMENSIONS=4 K=2 "
          "MISSING_RATE=0.2 RATE_PER_SECOND=200 QOS=0 BUILD_IMAGE=0 "
          "scripts/e2e-benchmark.sh && "
          f"python3 scripts/validate-e2e.py --expected-messages {expected} --expected-queries 1",
      ]
      profile = f"raw ({dataset})"
    else:
      objects = 100
      queries = 2
      max_events = objects * queries
      expected = max_events * topics if dataset == "all" else max_events
      env.update({"OBJECTS": str(objects), "QUERIES": str(queries), "EXPECTED_MESSAGES": str(expected), "DATASET": dataset})
      profile = f"default ({dataset})"
      command = ["just", "test-all"]
"""

content = re.sub(r'\ndef start_test\(profile, dataset="synthetic"\):.*?command = \["just", "test-all"\]\n', replacer, content, flags=re.DOTALL)

with open("scripts/monitor.py", "w") as f:
    f.write(content)
