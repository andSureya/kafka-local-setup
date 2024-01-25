FROM python:3.10-slim-buster


RUN mkdir -p /app


# Copy application files
COPY consume_schedule.py /app/consume_schedule.py


# SET working directory
WORKDIR /app

# Install dependencies
RUN pip3 install --upgrade pip && \
    pip3 install kafka-python==2.0.2

# Execute application
CMD python3 consume_schedule.py