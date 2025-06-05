FROM python:3.9-slim

WORKDIR /app

COPY . .
RUN apt-get update && apt-get install -y build-essential python3-dev

RUN pip install --no-cache-dir build && \
    python -m build && \
    pip install dist/*.whl && \
    rm dist/*.whl

ENV PYTHONPATH=/app:$PYTHONPATH

ENTRYPOINT ["hemera"]