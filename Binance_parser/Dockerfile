FROM python:3.9

WORKDIR /usr/app/src

ADD parse.py .

RUN pip install requests psycopg2-binary urllib3 python-dotenv

CMD [ "python", "-u", "./binance_parser.py"]