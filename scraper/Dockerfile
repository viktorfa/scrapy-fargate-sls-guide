FROM python:3.7

WORKDIR /usr/src/app

COPY scrapy.cfg ./
COPY launcher.py ./
COPY my_sls_scraper ./my_sls_scraper
COPY requirements.txt ./

RUN pip install boto3 --quiet
RUN pip install -r requirements.txt --quiet

CMD ["python3", "./launcher.py"]
