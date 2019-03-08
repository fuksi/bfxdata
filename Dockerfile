FROM python:3.6.5-alpine3.7

RUN apk update && \
    apk add --virtual build-deps gcc python-dev musl-dev && \
    apk add postgresql-dev

RUN mkdir /data

RUN pip install pipenv

WORKDIR /app

COPY Pipfile Pipfile

RUN pipenv install --skip-lock

COPY . /app