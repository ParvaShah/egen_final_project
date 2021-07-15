FROM ubuntu:18.04
WORKDIR /code

RUN apt-get -y update && \
    apt-get -y install python3.7 python3-pip

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt

EXPOSE 7007
COPY . .
CMD [ "python3", "init.py" ]