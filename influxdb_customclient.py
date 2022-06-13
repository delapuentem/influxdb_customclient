#!/usr/bin/env python3
import influxdb, requests, itertools, time, datetime, re

class InfluxDBCustomClient(influxdb.InfluxDBClient):
    def __init__(self, host, port, username, password, ssl=False, verify_ssl=False):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.ssl = ssl
        self.verify_ssl = verify_ssl
        self.influxdb_client = influxdb.InfluxDBClient(
            host=self.host, port=self.port, username=self.username, password=self.password, ssl=self.ssl, verify_ssl=self.verify_ssl)
        # Hereda de InfluxDBClient. Por lo que incluye todas las funciones de InfluxDBClient
        super().__init__(host, port, username, password, ssl=self.ssl, verify_ssl=self.verify_ssl)

    def __regex_match(self, fecha_string):
        '''Comprueba que el string que se pasa como parametro contenga una fecha que haga match con el regex especificado, y devuelve una diccionario con el regex que ha hecho match y su formato de fecha correspondiente'''
        # Formatos de fecha aceptados con los que va a comprobar si hace match o no la fecha
        formatos_aceptados = (
            ("\d{4}-[01]\d-[0-3]\dT[0-2]\d:[0-5]\d:[0-5]\d", "%Y-%m-%dT%H:%M:%S"),
            ("\d{4}-[01]\d-[0-3]\d\s[0-2]\d:[0-5]\d:[0-5]\d", "%Y-%m-%d %H:%M:%S"),
        )    
        # Recorre los formatos aceptados y si el string que se le pasa como argumento hace match con algún formato, devuelve el regex y formato correspondiente
        try:
            formato_fecha = next(filter(lambda formato: re.findall(formato[0], fecha_string), formatos_aceptados))
        except StopIteration:
            print(f"ERROR '{fecha_string}' no hace match con los formatos aceptados")
        # Devuelve una tupla con el regex que hace match con la fecha string que hems pasado, y el formato fecha correspondiente con ese regex que usaremos para pasarlo con sfrtime a objeto de tiempo
        return dict(match=True, formato_fecha=formato_fecha)        

    def __normalizar_fecha(self, fecha_string, **kwars):
        '''A partir de un string que contenga una fecha que haga match con el regex especificado, aplica la zona horaria y devuelve la fecha normalizada. Por defecto, el formato de salida es %Y-%m-%d %H:%M:%S, pero se puede especificar cualquier formato de salida.
        Por defecto, la zona horaria es +0, pero se puede ajustar con numero positivos o negativos.'''

        # Argumentos por defecto
        defaultargs = dict(formato_salida= '%Y-%m-%d %H:%M:%S', zona_horaria = +0,)
        # Argumentos por defecto. Si no se le pasa parametro coge como defecto el segundo parameto del get
        formato_salida = kwars.get('formato_salida', defaultargs['formato_salida'])
        zona_horaria = kwars.get('zona_horaria', defaultargs['zona_horaria'])

        # Formato regex y formato fecha
        formato_fecha_regex = self.__regex_match(fecha_string)['formato_fecha'][0] # Regex
        formato_fecha_fecha = self.__regex_match(fecha_string)['formato_fecha'][1]  # Formato de la fecha

        # Extramos la fecha del string que se le pasa como argumento a la funcion
        fecha = re.findall(formato_fecha_regex, fecha_string)[0]

        # Parseamos la fecha con el formato correspondiente para convertir la fecha del string en un objeto de tiempo y ajustamos la zona horaria
        fecha_objeto =  datetime.datetime.strptime(str(fecha), str(formato_fecha_fecha)) + datetime.timedelta(hours=zona_horaria)

        # Aplicamos el formato de salida
        fecha_output = fecha_objeto.strftime(formato_salida)

        return fecha_output           

    def influxdb_databases(self, exclude_databases=()):
        '''Return a tuple with the databases of Influxdb'''
        # Databases -> Excluding databases in exclude_databases var
        try:
            influxdb_databases = tuple(map(lambda item: item['name'], filter(lambda item: item['name'] not in exclude_databases, self.influxdb_client.get_list_database())))
            return influxdb_databases
        except influxdb.exceptions.InfluxDBClientError:
            print(f"ERROR Authorization failed with user {self.username} and password {self.password}")
        except requests.exceptions.ConnectionError:
            print(f"ERROR Failed to estalish connection to {self.host}:{self.port}")

    def influxdb_measurements(self, influxdb_databases):
        '''Create a dict of lists of database and measurement as a list of tuples.'''
        influxdb_measurements = []
        # For each database ...
        for database in influxdb_databases:
            # Access into that database, and create a list of tuples, with key=database_name and value=measurement name, and merge into a list
            self.influxdb_client.switch_database(database)
            try:
                measurement_list = map(lambda item: item[0],  self.influxdb_client.query('show measurements').raw['series'][0]['values'])
                influxdb_measurements += list(map(lambda measurement: (database, measurement), measurement_list))
            except IndexError:
               print(f"ERROR Database {database} does not contain any measurement value")

        # Create an empty dictionary to store the data
        measurements_dict = {}
        # Define a func variable to specify the grouping key
        group_key = lambda x: x[0]
        # Group measurements by databases in a dictionary
        for key, group in itertools.groupby(sorted(influxdb_measurements, key=group_key), group_key):
            measurements_dict[key] = list(group)
        return measurements_dict

    def influxdb_last_metric_received(self, database, measurement, interval=60):
        '''For a specific database measurement, know when the last metric was received. Last x minutes.'''
        # Set interval, default is last 1 hours
        query_time_to = datetime.datetime.today()
        query_time_since = datetime.datetime.today() - datetime.timedelta(minutes=interval)
        # Select database
        self.influxdb_client.switch_database(database)
        # Run query and get the most recent timestamp, if exists
        query = f"SELECT * FROM \"{measurement}\" WHERE TIME > '{query_time_since}' AND TIME < '{query_time_to}'  GROUP BY * ORDER BY DESC LIMIT 1"
        # If query has results:
        query_result = self.influxdb_client.query(query).raw['series']
        if len(query_result) > 0:
            result_code=0
            last_metric_timestamp = query_result[0]['values'][0][0]
            last_metric_since_minutes = (int(time.mktime(time.strptime(str(self.__normalizar_fecha(fecha_string=str(datetime.datetime.today()))), '%Y-%m-%d %H:%M:%S'))) - int(time.mktime(time.strptime(str(self.__normalizar_fecha(fecha_string=str(last_metric_timestamp))), '%Y-%m-%d %H:%M:%S')))) / 60
            return dict(database=database, measurement=measurement, result_code=result_code, interval=interval, last_metric_timestamp=last_metric_timestamp, last_metric=last_metric_since_minutes)
        else:
            result_code=1
            return dict(database=database, measurement=measurement, result_code=result_code, interval=interval)

    def influxdb_last_metric_received_list(self, influxdb_measurements, interval=60):
        '''Return a list of dicts with detail of last metric reveived for each database and measurement'''
        influxdb_last_metric_received_list = []
        for database in influxdb_measurements:
            for measurement in influxdb_measurements[database]:
                #print(f"{database} {measurement[1]}")
                try:
                    result = self.influxdb_last_metric_received(database=database, measurement=measurement[1], interval=interval)
                    influxdb_last_metric_received_list.append(result)
                except influxdb.exceptions.InfluxDBClientError:
                    print("ERROR in QUERY for function influxdb_last_metric_received()")
        return influxdb_last_metric_received_list