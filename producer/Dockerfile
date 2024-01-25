FROM python:3.10-slim-buster


RUN mkdir -p /app


# Copy application files
COPY emit_schedule.py /app/emit_schedule.py
COPY schedule.csv /app/emit_schedule.py



# SET working directory
WORKDIR /app

# Install dependencies
RUN pip3 install --upgrade pip && \
    pip3 install kafka-python==2.0.2

# Execute application
CMD python3 emit_schedule.py