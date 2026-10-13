FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY northwind ./northwind
COPY tests ./tests

ENV PYTHONUNBUFFERED=1
