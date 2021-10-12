import psycopg2
import psycopg2.extras
import sys
import time
import yaml
import datetime
import json

generic_sensor_format = \
    'CREATE TABLE %(table_name)s (\
        time         TIMESTAMP WITH TIME ZONE NOT NULL,\
        userID       VARCHAR(64),\
        value        NUMERIC,\
        unit         VARCHAR(64),\
        source       VARCHAR(128)\
    );'

gps_sensor_format = \
    'CREATE TABLE %(table_name)s (\
        time         TIMESTAMP WITH TIME ZONE NOT NULL,\
        userID       VARCHAR(64),\
        lat          NUMERIC,\
        long         NUMERIC,\
        unit         VARCHAR(64),\
        source       VARCHAR(128)\
    );'

acc_sensor_format = \
    'CREATE TABLE %(table_name)s (\
        time         TIMESTAMP WITH TIME ZONE NOT NULL,\
        userID       VARCHAR(64),\
        accx         NUMERIC,\
        accy         NUMERIC,\
        accz         NUMERIC,\
        grax         NUMERIC,\
        gray         NUMERIC,\
        graz         NUMERIC,\
        gyrx         NUMERIC,\
        gyry         NUMERIC,\
        gyrz         NUMERIC,\
        unit         VARCHAR(64),\
        source       VARCHAR(128)\
    );'

event_format = \
    'CREATE TABLE %(table_name)s (\
        time         TIMESTAMP WITH TIME ZONE NOT NULL,\
        start_time   TIMESTAMP WITH TIME ZONE NOT NULL,\
        end_time     TIMESTAMP WITH TIME ZONE NOT NULL,\
        userID       VARCHAR(64),\
        event_type   VARCHAR(64),\
        event_name   VARCHAR(64),\
        parameters   JSON,\
        datastreams  JSON\
    );'

activity_format = \
    'CREATE TABLE %(table_name)s (\
        time         TIMESTAMP WITH TIME ZONE NOT NULL,\
        userID       VARCHAR(64),\
        activity     VARCHAR(64),\
        unit         VARCHAR(64),\
        source       VARCHAR(128)\
    );'


table_dict = {
    'gps_table': 'gps_sensor_format',
    'hr_table': 'generic_sensor_format',
    'acc_table': 'acc_sensor_format',
    'event_table': 'event_format',
    'activity_table': 'activity_format'
}


class TimescaleDBModule:
    def __init__(self, yaml_file_path):
        with open(yaml_file_path) as file:
            # The FullLoader parameter handles the conversion from YAML
            # scalar values to Python the dictionary format
            database_info = yaml.load(file, Loader=yaml.SafeLoader)

        self.db_info = database_info
        self.params = {
          'dbname': database_info['dbname'],
          'user': database_info['user'],
          'password': database_info['password'],
          'host': database_info['host'],
          'port': database_info['port']
        }

        self.conn = psycopg2.connect(**self.params)


    def create_table(self):
        for tbl in table_dict:
            table_name = self.db_info[tbl]
            cur = self.conn.cursor()
            cur.execute("select exists(select * from information_schema.tables where table_name='%s')" % (table_name))
            if cur.fetchone()[0]:
                exist = True
                # print("TimescaleDBModule::create_table\n\tTable {} already exists".format(table_name))
            else:
                exist = False
                # print(f'TimescaleDBModule::create_table\n\tTable don\'t exist, creating table {table_name}')

            schema_type = table_dict[tbl]

            create_table_cmnd = eval(schema_type) % {'table_name': table_name}
            hyper_table_cmnd = f"SELECT create_hypertable('{table_name}', 'time');"
            if not exist:
                cur.execute(create_table_cmnd)
                cur.execute(hyper_table_cmnd)
                cur.close()
                self.conn.commit()
            else:
                cur.close()


    def delete_table(self):
        for tbl in table_dict:
            table_name = self.db_info[tbl]
            cur = self.conn.cursor()
            try:
                cur.execute(f'DROP TABLE IF EXISTS {table_name};')
                cur.close()
                self.conn.commit()
            except:
                cur.close()


    def select_events(self, time_start, time_end):
        return self.select_signals('event', time_start, time_end)

    def select_signals(self, signal_type, start_time, end_time):
        # signal_type can be gps, hr, acc or even events.
        table_name = self.db_info[signal_type + '_table']
        start = datetime.datetime.timestamp(start_time)
        end = datetime.datetime.timestamp(end_time)

        return self.execute_query(f'SELECT * FROM {table_name} WHERE time >= to_timestamp({start}) AND time <= to_timestamp({end})')


    def execute_query(self, q):
        cur = self.conn.cursor()
        try:
            cur.execute(q)
            results = cur.fetchall()
            cur.close()

            return results
        except:
            print(f'Execute query: {q} failed')
            return None


    def insert_event(self, start_time, end_time, userID: str, event_type: str, event_name: str, parameters: dict, datastreams: list):
        table_name = self.db_info['event_table']
        start_ts = datetime.datetime.timestamp(start_time)
        end_ts = datetime.datetime.timestamp(end_time)
        json_params = json.dumps(parameters)
        json_datastreams = json.dumps(datastreams)

        insert_cmnd = f"INSERT INTO {table_name} VALUES (\
        to_timestamp({start_ts}), to_timestamp({start_ts}), to_timestamp({end_ts}), '{userID}', '{event_type}', '{event_name}', '{json_params}', '{json_datastreams}'\
        ) RETURNING *;"

        cur = self.conn.cursor()
        cur.execute(insert_cmnd)
        cur.close()
        self.conn.commit()


    def insert_sensor(self, sensor_type: str, time: datetime.datetime, userID: str, value: float, unit: str, source: str):
        table_name = self.db_info[sensor_type + '_table']
        if sensor_type == 'acc':
            self.insert_acc(time, userID, value, unit, source)
            return
        elif sensor_type == 'gps':
            self.insert_gps(time, userID, value, unit, source)
            return
        timestamp_ms = datetime.datetime.timestamp(time)

        insert_cmnd = f"INSERT INTO {table_name} VALUES (to_timestamp({timestamp_ms}), '{userID}', {value}, '{unit}', '{source}') RETURNING *;"

        cur = self.conn.cursor()
        cur.execute(insert_cmnd)
        cur.close()
        self.conn.commit()


    def insert_acc(self, time: datetime.datetime, userID: str, values: list, unit: str, source: str):
        table_name = self.db_info['acc_table']
        timestamp_ms = datetime.datetime.timestamp(time)

        if len(values) != 9:
            print('Data has incorrect number of values')
            return None

        val_str = ''
        for val in values:
            val_str += str(val) + ', '

        insert_cmnd = f"INSERT INTO {table_name} VALUES (to_timestamp({timestamp_ms}), '{userID}', {val_str}'{unit}', '{source}') RETURNING *;"

        cur = self.conn.cursor()
        cur.execute(insert_cmnd)
        cur.close()
        self.conn.commit()


    def insert_gps(self, time: datetime.datetime, userID: str, values: list, unit: str, source: str):
        table_name = self.db_info['gps_table']
        timestamp_ms = datetime.datetime.timestamp(time)

        if len(values) != 2:
            print('Data has incorrect number of values')
            return None

        val_str = ''
        for val in values:
            val_str += str(val) + ', '

        insert_cmnd = f"INSERT INTO {table_name} VALUES (to_timestamp({timestamp_ms}), '{userID}', {val_str}'{unit}', '{source}') RETURNING *;"

        cur = self.conn.cursor()
        cur.execute(insert_cmnd)
        cur.close()
        self.conn.commit()


    def bulk_insert(self, sensor_type, time_list, userID_list, data, unit_list, source_list):
        table_name = self.db_info[sensor_type + '_table']
        cur = self.conn.cursor()
        str_time_list = []
        for time in time_list:
            timestamp_ms = datetime.datetime.timestamp(time)
            str_time_list.append(timestamp_ms)

        full_data = []
        for i in range(len(data)):
            full_data.append((str_time_list[i], userID_list[i]) + tuple(data[i]) + (unit_list[i], source_list[i]))

        template = ('to_timestamp(%s)', '%s') + ('%s',) * len(data[0]) + ('%s', '%s')
        template = '(' + ','.join(template) + ')'

        insert_query = f'INSERT INTO {table_name} VALUES %s'
        psycopg2.extras.execute_values (
            cur, insert_query, full_data, template=template, page_size=100
        )
        cur.close()
        self.conn.commit()
