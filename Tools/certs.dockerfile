FROM python:3.13.3-alpine3.21

RUN pip3 install cryptography
ADD gen_certs.py /gen_certs.py

WORKDIR /certs

CMD ["python3", "/gen_certs.py"]