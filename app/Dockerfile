# Use a lightweight Python image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Copy project files into the container
COPY . .

# Install dependencies
RUN pip install --no-cache-dir flask pika python-dotenv gunicorn

# Expose Flask port
EXPOSE 8080

# Run the app
CMD ["gunicorn", "-w", "4", "-b", "0.0.0.0:8080", "main:app"]
