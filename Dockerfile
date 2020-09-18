# FROM python:3.7-alpine
# FROM docker.io/prairielearn/centos7-python
FROM python:3.7.4
ENV PYTHONUNBUFFERED 1
RUN mkdir /code
WORKDIR /code
COPY ./requirements.txt /code/
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
COPY . /code