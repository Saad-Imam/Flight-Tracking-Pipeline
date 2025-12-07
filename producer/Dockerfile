FROM python:3.9-slim

WORKDIR /app

# Install dependencies
RUN pip install torch==2.0.1+cpu --index-url https://download.pytorch.org/whl/cpu && \
pip install pandas sdv kafka-python

# Copy the script AND the pre-trained model
COPY stream_producer.py .
COPY model.pkl .

# Command to run continuously
CMD ["python", "stream_producer.py"]