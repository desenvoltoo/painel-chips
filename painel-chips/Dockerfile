# ============================
# Base image
# ============================
FROM python:3.11-slim

# Disable output buffering
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TZ=America/Sao_Paulo

# Work directory
WORKDIR /app

# Install OS dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ libstdc++6 curl tzdata && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy your entire project
COPY . .

# Cloud Run listens on $PORT
ENV PORT=8080

# Start Flask (Waitress recommended)
CMD ["python", "app.py"]
