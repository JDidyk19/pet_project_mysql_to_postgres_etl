FROM python:3.14-slim
WORKDIR /usr/app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["python", "fake_data_generator.py"]