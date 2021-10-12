# syntax=docker/dockerfile:1

FROM apache/beam_python3.7_sdk

ENV PYTHONUNBUFFERED 1
WORKDIR /app
COPY . .

ENTRYPOINT ["python", "covid_aggregator.py"]
