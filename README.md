# eod_store

A local storage and API for end-of-day market data

## Technologies:

- Data is stored in **HDF** files
- **Flask** is used as an engine for API
- **Celery** for background tasks
- **Redis** as a backend and message broker for Celery 
- [alpha_vantage](https://github.com/RomelTorres/alpha_vantage) module for python is used to download historical stock market data

## Requirements:

- Docker and Docker-compose

## Installation:

1. Clone this repository to a desired location:
```sh
$ git clone https://github.com/olzhasar/eod_store.git
```
2. Move to the newly created repository folder:
```sh
$ cd eod_store
```
3. Create variables.env file and place your API keys there (you need to specify
   at least one):
```sh
$ cat > variables.env
ALPHA_VANTAGE_KEY=<your_key>
QUANDL_KEY=<your_key>
```
4. Finally, run docker containers by using:
```sh
$ docker-compose up
```

## Usage:

- To do

