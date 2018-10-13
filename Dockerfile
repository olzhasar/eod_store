FROM python:3.6
ADD . /mongo_quotes
WORKDIR /mongo_quotes
RUN pip install -r requirements.txt
