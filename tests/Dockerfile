FROM python:3.12-slim

WORKDIR /tests

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY conftest.py test_pipeline.py ./

CMD ["python", "-m", "pytest", "-v", "--color=yes"]
