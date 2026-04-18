ARG PYTHON_VERSION=3.12-slim

FROM python:${PYTHON_VERSION}

WORKDIR /app

ARG GIT_SHA
ENV GIT_SHA=${GIT_SHA}

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/
COPY data/ ./data/

# Create a script to run streamlit with Railway's PORT
RUN echo '#!/bin/sh\nstreamlit run app/main.py --server.port ${PORT:-8501} --server.address 0.0.0.0 --server.headless=true' > /start.sh && chmod +x /start.sh

EXPOSE 8501

CMD ["/start.sh"]
