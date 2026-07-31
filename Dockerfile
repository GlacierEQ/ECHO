FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV ECHO_DB=/data/echo.db
VOLUME ["/data"]

EXPOSE 8000

CMD ["uvicorn", "echo.main:app", "--host", "0.0.0.0", "--port", "8000"]
