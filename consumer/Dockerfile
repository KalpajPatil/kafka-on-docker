FROM python:3.13-slim
COPY sample_consumer.py /app/sample_consumer.py
WORKDIR /app
RUN pip install confluent-kafka
CMD ["python","sample_consumer.py"]
