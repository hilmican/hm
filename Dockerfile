FROM python:3.10-alpine

ENV PYTHONDONTWRITEBYTECODE=1 \
	PYTHONUNBUFFERED=1

WORKDIR /app

# Minimal tooling (curl, ping via busybox-extras)
RUN apk add --no-cache curl busybox-extras

# Copy requirements and install
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy project
COPY . .

# Ensure data dir exists for media/thumbs, etc.
RUN mkdir -p /app/data

EXPOSE 8388

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8388", "--workers", "2"]
