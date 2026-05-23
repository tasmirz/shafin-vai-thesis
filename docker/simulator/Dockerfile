FROM python:3.12-slim

WORKDIR /app

COPY requirements-simulator.txt /app/requirements-simulator.txt
RUN python -m venv /opt/simulator-venv \
    && /opt/simulator-venv/bin/pip install --no-cache-dir -r /app/requirements-simulator.txt

COPY scripts/simulator.py /app/scripts/simulator.py
COPY datasets-raw /app/datasets-raw

ENTRYPOINT ["/opt/simulator-venv/bin/python", "/app/scripts/simulator.py"]
