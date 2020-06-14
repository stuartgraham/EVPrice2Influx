
# EV Price Watch to InfluxDB
Watches EV Prices and publishes to InfluxDB on periodic basis

### settings.py
Create settings.py from settings.py.example
| Settings | Description | Inputs |
| :----: | --- | --- |
| `LIVE_CONN` | Enables live lookups on website | `True` |
| `INFLUX_HOST` | InfluxDB host | `influx.test.local` |
| `INFLUX_HOST_PORT` | InfluxDB port  | `8086` |
| `INFLUX_DATABASE` | InfluxDB databse  | `test` |
| `RUNMINS` | Run every x mins | `720` |

### Requirements
```sh
pip install -p requirements.txt
```

### Execution 
```sh
python3 .\main.py
```

### Docker Compose
```sh 
evprice2influx:
    image: stuartgraham/evprice2influx
    restart: always
    container_name: evprice2influx
    environment:
        - LIVE_CONN=True
        - INFLUX_HOST=influx.test.local
        - INFLUX_HOST_PORT=8086
        - INFLUX_DATABASE=ev_prices
        - RUNMINS=720
```