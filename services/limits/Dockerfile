FROM python:3.11.9-slim-bookworm

RUN apt-get update \
    && apt-get upgrade -y \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt /app/
RUN pip install --no-cache-dir --upgrade pip==25.3 setuptools==78.1.1 \
    && pip install --no-cache-dir -r requirements.txt
COPY . /app
CMD ["uvicorn","main:app","--host","0.0.0.0","--port","8080"]
