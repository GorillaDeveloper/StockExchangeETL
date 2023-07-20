@echo on

docker rm psxetl
docker rmi psx-etl-image
docker build -t psx-etl-image -f Dockerfile .
docker run --network=transferdatafrompsxtobq_etl_scrapperappnetwork --name psxetl psx-etl-image

pause