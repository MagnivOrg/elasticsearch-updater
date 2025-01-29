FROM python:3.11

WORKDIR /

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "update_elasticsearch.py"]