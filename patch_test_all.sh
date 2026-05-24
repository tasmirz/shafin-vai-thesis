import re

with open("scripts/test-all.sh", "r") as f:
    content = f.read()

def replacer(match):
    return """if [[ "$DATASET" == "all" ]]; then
  EXPECTED_MESSAGES="${EXPECTED_MESSAGES:-$((MAX_EVENTS * 3))}"
  MONITOR_ENV=(
    "TOPIC_MAPPINGS=thesis/raw/intel=thesis.raw.intel,thesis/raw/pump=thesis.raw.pump,thesis/raw/gas=thesis.raw.gas"
    "ACTION_IDS=kafka_producer:raw_incomplete_to_kafka_thesis_raw_intel,kafka_producer:raw_incomplete_to_kafka_thesis_raw_pump,kafka_producer:raw_incomplete_to_kafka_thesis_raw_gas"
  )
elif [[ "$DATASET" == "intel" || "$DATASET" == "pump" || "$DATASET" == "gas" ]]; then
  EXPECTED_MESSAGES="${EXPECTED_MESSAGES:-$((OBJECTS * QUERIES))}"
  MONITOR_ENV=(
    "TOPIC_MAPPINGS=thesis/raw/$DATASET=thesis.raw.$DATASET"
    "ACTION_IDS=kafka_producer:raw_incomplete_to_kafka_thesis_raw_$DATASET"
  )
else
  EXPECTED_MESSAGES="${EXPECTED_MESSAGES:-$((OBJECTS * QUERIES))}"
  MONITOR_ENV=()
fi"""

content = re.sub(r'if \[\[ "\$DATASET" == "all" \]\]; then.*?MONITOR_ENV=\(\)\nfi', replacer, content, flags=re.DOTALL)

with open("scripts/test-all.sh", "w") as f:
    f.write(content)
