FROM python:3.13-slim
COPY sample_producer.py /app/sample_producer.py
WORKDIR /app
RUN pip install confluent-kafka
CMD ["python","sample_producer.py"]