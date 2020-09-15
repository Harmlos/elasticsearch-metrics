FROM python:3

RUN pip install pyyaml
RUN pip install requests

ADD script.py /data/elastic_metrics/
ADD config.yml /data/elastic_metrics/config.yml

CMD [ "python", "/data/elastic_metrics/script.py" ]
