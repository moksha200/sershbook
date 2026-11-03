FROM mcr.microsoft.com/playwright/python:v1.64.1-noble

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright \
    PORT=8090 \
    LOW_MEMORY=1

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /app/requirements.txt && \
    python -m playwright install chromium

COPY index.py main.py mcp_stdio_bridge.py test_suite.py mcp_configs.json /app/

EXPOSE 8090

CMD ["sh", "-c", "uvicorn index:app --host 0.0.0.0 --port ${PORT:-8090}"]
