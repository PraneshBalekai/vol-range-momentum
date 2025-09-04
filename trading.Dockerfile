FROM python:3.12-bookworm

RUN pip install poetry

ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1 \
    POETRY_CACHE_DIR=/tmp/poetry_cache

WORKDIR /app

COPY . .

WORKDIR /app/trading
RUN poetry install

ENTRYPOINT ["poetry", "run", "python", "trading/intraday_momentum.py", "--config-path", "configs/intraday_momentum_spy.json", "--docker-run"]
