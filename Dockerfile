# Use an official Python runtime
FROM python:3.11-slim

# Install dependencies
WORKDIR /app
# Update pip to latest version
RUN pip install --upgrade pip
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy code
COPY . .

# Expose port
ENV PORT=8080
CMD exec gunicorn --bind :$PORT main:app
