# Use an official Python runtime as a parent image
FROM python:3.9
WORKDIR /app
COPY . /app
RUN pip install -r requirements.txt

EXPOSE 8501
CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true"]