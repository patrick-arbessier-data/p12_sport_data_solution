# Image runner Python pour exécuter les scripts du projet dans un conteneur éphémère.
# Cible : Python 3.13 + libs nécessaires aux scripts (ingestion, config, appels HTTP).
FROM python:3.13-slim

# Réglages de base : logs immédiats + pas de .pyc.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Dépendances système minimales (certificats HTTPS).
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Installer les dépendances depuis le fichier projet (root)
COPY requirements.txt /tmp/requirements.txt
RUN python -m pip install --no-cache-dir -U pip \
    && python -m pip install --no-cache-dir -r /tmp/requirements.txt


# Le repo sera monté en volume dans /workspace par docker compose.
WORKDIR /workspace

# Commande par défaut (sera généralement surchargée par Kestra).
CMD ["python", "-V"]
