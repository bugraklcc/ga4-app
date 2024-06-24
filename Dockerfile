FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY src /app/src

RUN chmod -R 755 /app/src/csv

ENV PYTHONPATH "${PYTHONPATH}:/app"

EXPOSE 8080

CMD ["python", "./src/process_ga4_data.py"]

