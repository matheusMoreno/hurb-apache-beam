# syntax=docker/dockerfile:1

# Python 3.7 build from the Apache Beam team
FROM apache/beam_python3.7_sdk

ENV PYTHONUNBUFFERED 1
WORKDIR /app
COPY . .

ENTRYPOINT ["python", "covid_aggregator.py"]
