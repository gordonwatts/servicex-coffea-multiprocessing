FROM python:3.7-buster

RUN  apt-get update -y; apt-get install -y gcc python3-dev
COPY image_requirements.txt ./requirements.txt
RUN pip install -r requirements.txt && pip install funcx-endpoint
COPY setup.py /opt/sx_multi/
COPY src /opt/sx_multi/
WORKDIR /opt/sx_multi
RUN /usr/local/bin/python -m pip install .


