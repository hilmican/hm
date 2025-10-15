FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
	PYTHONUNBUFFERED=1

WORKDIR /app

# System deps for openpyxl, etc.
RUN apt-get update && apt-get install -y --no-install-recommends \
	build-essential \
	&& rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy project
COPY . .

# Ensure data dir exists for SQLite
RUN mkdir -p /app/data

EXPOSE 8388

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8388"]
