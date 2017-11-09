FROM python:3.6

MAINTAINER walter traspadini

WORKDIR /usr/src/app

COPY ./requirements.txt ./
COPY ./consumer.py ./
RUN pip install --no-cache-dir -r requirements.txt

CMD [ "python", "./consumer.py" ]