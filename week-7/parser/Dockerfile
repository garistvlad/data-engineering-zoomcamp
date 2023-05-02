FROM python:3.11

WORKDIR /parser

COPY ./requirements.txt requirements.txt

RUN pip3 install --no-cache-dir --upgrade pip
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . /parser/
