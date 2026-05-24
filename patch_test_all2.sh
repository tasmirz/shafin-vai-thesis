import re

with open("scripts/test-all.sh", "r") as f:
    content = f.read()

content = content.replace("BUILD_IMAGE=0 \\\nscripts/e2e-benchmark.sh", "BUILD_IMAGE=0 \\\nenv \"${MONITOR_ENV[@]}\" \\\nscripts/e2e-benchmark.sh")

with open("scripts/test-all.sh", "w") as f:
    f.write(content)
