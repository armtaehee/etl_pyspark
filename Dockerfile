FROM python:3.7-buster
RUN apt-get update

# Setup Java and JAVA_HOME
WORKDIR /opt/
RUN wget --no-clobber --no-cookies --no-check-certificate --header "Cookie: oraclelicense=accept-securebackup-cookie" https://javadl.oracle.com/webapps/download/GetFile/1.8.0_301-b09/d3c52aa6bfa54d3ca74e617f18309292/linux-i586/jdk-8u301-linux-x64.tar.gz
RUN tar zxvf jdk-8u301-linux-x64.tar.gz
RUN wget -O /opt/postgresql-jdbc.jar https://jdbc.postgresql.org/download/postgresql-42.6.0.jar
ENV JAVA_HOME /opt/jdk1.8.0_301
RUN export JAVA_HOME

# In case you want to use poetry, uncomment this


# UNCOMMENT ↓ for poetry
# RUN pip install poetry

WORKDIR /app/sertis-etl-app
# Ideally this step should be a git clone from remote repo, which would ensure smarter CACHE handling for Docker layers
ADD requirements.txt ./

# UNCOMMENT ↓ for poetry
# COPY poetry.lock pyproject.toml ./

# install requirements
RUN pip3 install -r requirements.txt
# UNCOMMENT ↓ for poetry
# RUN poetry install  --no-root

# finally copy the repository, ideally should be git clone
COPY  . /app/sertis-etl-app
