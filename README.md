# InfluxDB Custom Client
Custom library inheriting from influxdb 1.x python client https://github.com/influxdata/influxdb-python

## How to use

### Make a connection

Make sure that the user has read permissions in all the databases that you want to extract the information

```python
#!/usr/bin/env python3
import influxdb_customclient

# Make a connection
influxdb_client = influxdb_customclient.InfluxDBCustomClient(host='domain/ip_address', port=8086, username='my_username', password='my_password', ssl=False, verify_ssl=False)
```

### Get all databases
Return a tuple with the databases of Influxdb.

```python
# Exclude databases (optional)
exclude_databases = ('database1','database2')

# Excluding databases 
influxdb_databases = influxdb_client.influxdb_databases()
# Not excluding databases (default)
influxdb_databases = influxdb_client.influxdb_databases(exclude_databases=exclude_databases)
```

### Get all measurements for each database
Return a dict of lists of database and measurement as a list of tuples.

```python
influxdb_measurements = influxdb_client.influxdb_measurements(influxdb_databases=influxdb_databases)
```

Accessing data.

```python
for database in influxdb_measurements:
    for data in influxdb_measurements[database]:
        print(f"database: {data[0]} measurement: {data[1]}")
```

### Get the last metric of specific database and measurement
For a specific database measurement, know when the last metric was received. Last x minutes.

```python
# Set the interval in minutes (optional). if not specified, 60 minutes by default
interval = 2400

# Specifying an interval
last_metric = influxdb_client.influxdb_last_metric_received(database='my_database', measurement='my_measurement', interval=interval)
# Without specifying an interval (default)
last_metric = influxdb_client.influxdb_last_metric_received(database='my_database', measurement='my_measurement')
```

### Get the last metric for each measurement  of database list
Return a list of dicts with detail of last metric reveived for each database and measurement

```python
# Set the interval in minutes (optional). if not specified, 10 minutes by default
interval = 2400

# Specifying an interval
last_metric_list = influxdb_client.influxdb_last_metric_received_list(influxdb_measurements=influxdb_measurements, interval=interval)
# Without specifying an interval (default)
last_metric_list = influxdb_client.influxdb_last_metric_received_list(influxdb_measurements=influxdb_measurements)
```

## Metrics

- influxdb_customclient
	- tags:
		- database
		- measurement
	- fields:
		- result_code (int, 0-> has metrics, 1->has no metrics)
		- interval (int, minutes)
		- last_metric_timestamp (time of last metric, 2022-06-12T09:00:11Z)
		- last_metric (float, time in minutes since last metric)
