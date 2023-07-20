# Stage 1: Base image with system dependencies and Python packages
FROM python:3 AS base

WORKDIR /usr/app/src/psx-etl/

COPY requirements.txt ./
RUN pip3 install -r requirements.txt

# Stage 2: Final image with code
FROM base AS final

# VOLUME E:\1_DataEngineering\4_DataScrapping\appdata:/usr/app/src/nahdionline.com/data


COPY scripts /usr/app/src/psx-etl/scripts
COPY MainETL.py /usr/app/src/psx-etl/
COPY gcs-private-key.json /usr/app/src/psx-etl/

CMD ["python3", "MainETL.py", "--startdate", "2013-11-04", "--enddate", "2013-11-10"]