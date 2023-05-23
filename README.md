# StockExchangeETL
This private repository contains the psx stock exchange etl python pipeline, this etl pipeline code downloads the pakistan stock exchange data from 1-jan-2014 in zip file extract it and fix the csv file and remove all unnecessary commas that causes parsing errors and uploads it to GCS bucket and then transfer the csv data to Big Query.
