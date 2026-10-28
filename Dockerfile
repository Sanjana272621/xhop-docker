FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY service.py loadgen.py dashboard.py dashboard.html ./
ENV PYTHONUNBUFFERED=1
CMD ["python", "service.py"]
