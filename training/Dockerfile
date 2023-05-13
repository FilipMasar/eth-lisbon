FROM python:3.8-slim

COPY local_training.py /app/local_training.py

WORKDIR /app

COPY requirements.txt /app

RUN pip3 install -r requirements.txt

ADD local_training.py .

CMD ["python3", "local_training.py", "/inputs", "/outputs"]