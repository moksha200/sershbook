# Image officielle Microsoft incluant Python et Chromium déjà configuré
FROM mcr.microsoft.com/playwright/python:v1.40.0-jammy

WORKDIR /app

# Installation des dépendances Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copie du code index.py
COPY . .

# Variables d'environnement pour Render (Port 10000 par défaut)
EXPOSE 10000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "10000"]
