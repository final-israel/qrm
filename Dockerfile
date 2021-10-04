FROM python:3.9

COPY qrm_server/requirements.txt requirements.txt
COPY qrm_server/test_requirements.txt test_requirements.txt
RUN apt-get install redis-server
RUN python -m pip install -r test_requirements.txt
RUN python -m pip install -r requirements.txt