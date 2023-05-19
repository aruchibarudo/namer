ARG REGISTRY
ARG IMAGE=python:3.10-alpine

FROM ${REGISTRY}${IMAGE}

RUN wget http://repos.techpark.local/repository/stuff/CA/Techpark_CA_chain.pem \
    -O /usr/local/share/ca-certificates/Techpark_CA_chain.crt
    
RUN wget http://repos.techpark.local/repository/repos/alpine/repositories \
    -O /etc/apk/repositories

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt --index-url http://repos.techpark.local/repository/pypi-all/simple --trust repos.techpark.local

COPY . /namer

RUN chown -R nobody:nobody /namer

USER nobody

ENV HOME /namer

WORKDIR /namer

CMD ["python", "app.py"]