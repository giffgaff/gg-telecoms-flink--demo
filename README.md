Popular times: Real-time data on saturation for a supermarket
=====================
This application is responsible to be able to provide real-time data on saturation for a supermarket that takes sensor data of customers entering and exiting the store. 

## To Run this python script

`docker build -t "kafkacsv" .`

`docker run -it --rm \
-v $PWD:/home \
--network=host \
kafkacsv python bin/sendStream.py data/data.csv my-stream --speed 10`

## To Run this python script with docker-compose


