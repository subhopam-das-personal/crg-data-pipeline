ARG PYTHON_VERSION=3.12-slim

FROM python:${PYTHON_VERSION}

WORKDIR /app

ARG GIT_SHA
ENV GIT_SHA=${GIT_SHA}

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/
COPY data/ ./data/

EXPOSE 8501

CMD ["streamlit", "run", "app/main.py", "--server.port=8501", "--server.address=0.0.0.0", "--server.headless=true"]
