import sqlite3
import configparser
import psycopg2
import psycopg2.extras
import pandas
import sqlalchemy


class Database:
    def commit(self):
        self.connection.commit()

    def close(self):
        self.connection.close()


class PostgresDatabase(Database):
    def __init__(self, config_file, config_name):
        self.user = None
        self.pwd = None
        self.connection = None
        self.cursor = None
        self.dict_cursor = None
        self.host = None
        self.db_name = None
        self.uri = None
        self.load_config(config_file, config_name)
        self.connect()
        self.db_type = 'postgres'
        self.placeholder = '%s'

    def load_config(self, config_file, config_name):
        config = configparser.ConfigParser(allow_no_value=True)
        config.optionxform = str
        config.read(config_file)
        self.host = config[config_name]['host']
        self.user = config[config_name]['user']
        self.pwd = config[config_name]['pwd']
        self.db_name = config[config_name]['database']
        self.uri = 'postgresql+psycopg2://{user}:{pwd}@{host}/{db_name}'
        self.uri = self.uri.format(user=self.user, pwd=self.pwd, host=self.host, db_name=self.db_name)

    def connect(self):
        connection_str = "host='{}' dbname='{}' user='{}' password='{}'".format(self.host, self.db_name, self.user, self.pwd)
        self.connection = psycopg2.connect(connection_str)
        self.cursor = self.connection.cursor()
        self.dict_cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)


class SQLiteDatabase(Database):
    def __init__(self, db_path):
        self.connection = sqlite3.connect(db_path)
        self.cursor = self.connection.cursor()
        self.uri = 'sqlite:///{db_path}'.format(db_path=db_path)
        self.db_type = 'sqlite'
        self.placeholder = '?'


class DBTable:
    def __init__(self, database, schema=None):
        self.database = database
        self.schema = schema
        if self.schema is not None:
            self.full_table_name = self.schema + '.' + self.table_name
        else:
            self.full_table_name = self.table_name

    def clear(self):
        query = 'DELETE FROM {table_name}'
        query = query.format(table_name=self.full_table_name)
        self.database.cursor.execute(query)
        self.database.commit()

    def drop(self):
        query = 'DROP TABLE IF EXISTS {table_name}'.format(table_name=self.full_table_name)
        query = query.format(table_name=self.full_table_name)
        self.database.cursor.execute(query)
        self.database.commit()
        
    def commit(self):
        self.database.commit()

    def select_all(self):
        query = 'SELECT * FROM {table_name}'.format(table_name=self.full_table_name)
        self.database.cursor.execute(query)
        ret = self.database.cursor.fetchall()
        return ret

    def from_dataframe(self, df):
        engine = sqlalchemy.create_engine(self.database.uri)
        df.to_sql(name=self.full_table_name, con=engine, schema=self.schema, if_exists='replace', index=True)

    def to_dataframe(self, query=None):
        if query is None:
            query = 'SELECT * FROM {table_name}'
        query = query.format(table_name=self.full_table_name)
        engine = sqlalchemy.create_engine(self.database.uri)
        df = pandas.read_sql(sql=query, con=engine)
        return df

    def add_pkey(self):
        query = 'ALTER TABLE {full_table_name} ADD CONSTRAINT {table_name}_pkey PRIMARY KEY(idx);'
        query = query.format(full_table_name=self.full_table_name, table_name=self.table_name)
        self.database.cursor.execute(query)
        self.database.commit()


class VesselTable(DBTable):
    table_name = 'vessels'

    def upsert_vessel(self, ship_info):
        if self.already_exists(ship_info):
            self.update_vessel(ship_info)
        else:
            self.insert(ship_info)

    def already_exists(self, ship_info):
        if ship_info['imo'] is not None:
            query = 'SELECT idx FROM {table_name} WHERE imo = {val}'
            query = query.format(table_name=self.full_table_name, val=self.database.placeholder)
            self.database.cursor.execute(query, (ship_info['imo'],))
        else:
            query = 'SELECT idx FROM {table_name} WHERE mmsi = {val}'
            query = query.format(table_name=self.full_table_name, val=self.database.placeholder)
            self.database.cursor.execute(query, (ship_info['mmsi'],))
        ret = self.database.cursor.fetchall()
        if len(ret) > 0:
            return True
        else:
            return False

    def delete_vessel(self, ship_info):
        if ship_info['mmsi']:
            mmsi = ship_info['mmsi']
            self.delete_by_mmsi(mmsi)
        else:
            imo = ship_info['imo']
            self.delete_by_imo(imo)
            
    def delete_by_imo(self, imo):
        query = 'DELETE FROM {table_name} ' \
                'WHERE imo = {val}'
        query = query.format(table_name=self.full_table_name, val=self.database.placeholder)
        self.database.cursor.execute(query, (imo,))

    def delete_by_mmsi(self, mmsi):
        query = 'DELETE FROM {table_name} ' \
                'WHERE mmsi = {val}'
        query = query.format(table_name=self.full_table_name, val=self.database.placeholder)
        self.database.cursor.execute(query, (mmsi,))

    def insert(self, ship_info):
        query = 'INSERT INTO {table_name} (mmsi, imo, name, country, ship_type, gt, built, length, width) ' \
                'VALUES ({val},{val},{val},{val},{val},{val},{val},{val},{val})'
        query = query.format(table_name=self.full_table_name, val=self.database.placeholder)
        mmsi = ship_info['mmsi']
        imo = ship_info['imo']
        name = ship_info['name']
        country = ship_info['country']
        ship_type = ship_info['ship_type']
        gt = ship_info['gt']
        built = ship_info['built']
        length = ship_info['length']
        width = ship_info['width']
        self.database.cursor.execute(query, (mmsi, imo, name, country, ship_type, gt, built, length, width))

    def update_vessel(self, ship_info):
        if ship_info['mmsi']:
            self.update_by_mmsi(ship_info)
        elif ship_info['imo']:
            self.update_by_imo(ship_info)

    def update_by_mmsi(self, ship_info):
        query = 'UPDATE {table_name} ' \
                'SET ' \
                'imo={val},name={val},country={val},ship_type={val},gt={val},built={val},length={val},width={val} ' \
                'WHERE mmsi={val}'
        query = query.format(table_name=self.full_table_name, val=self.database.placeholder)
        imo = ship_info['imo']
        name = ship_info['name']
        country = ship_info['country']
        ship_type = ship_info['ship_type']
        gt = ship_info['gt']
        built = ship_info['built']
        length = ship_info['length']
        width = ship_info['width']
        mmsi = ship_info['mmsi']
        self.database.cursor.execute(query, (imo, name, country, ship_type, gt, built, length, width, mmsi))

    def update_by_imo(self, ship_info):
        query = 'UPDATE {table_name} ' \
                'SET ' \
                'mmsi={val},name={val},country={val},ship_type={val},gt={val},built={val},length={val},width={val} ' \
                'WHERE imo = {val}'
        query = query.format(table_name=self.full_table_name, val=self.database.placeholder)
        mmsi = ship_info['mmsi']
        name = ship_info['name']
        country = ship_info['country']
        ship_type = ship_info['ship_type']
        gt = ship_info['gt']
        built = ship_info['built']
        length = ship_info['length']
        width = ship_info['width']
        imo = ship_info['imo']
        self.database.cursor.execute(query, (mmsi, name, country, ship_type, gt, built, length, width, imo))

    def batch_insert(self, ret):
        query = 'INSERT INTO {table_name}(idx, mmsi, imo, name, ship_type, gt, built, length, width, country)'\
                'VALUES {val}'
        query = query.format(table_name=self.full_table_name, val=self.database.placeholder)
        psycopg2.extras.execute_values(self.database.cursor, query, ret)

    def create(self):
        query = 'CREATE TABLE {table_name}(' \
                'idx serial,' \
                'mmsi integer,' \
                'imo integer,' \
                'name text,' \
                'ship_type text,' \
                'gt double precision,' \
                'built integer,' \
                'length integer,' \
                'width integer,' \
                'country text)'
        query = query.format(table_name=self.full_table_name)
        self.database.cursor.execute(query)
        self.database.commit()


class PositionTable(DBTable):
    table_name = 'positions'

    def upsert_position(self, position_info):
        self.delete_position(position_info)
        self.add_position(position_info)

    def delete_position(self, position_info):
        mmsi = position_info['mmsi']
        date = position_info['date']
        imo = position_info['imo']
        if mmsi:
            self.delete_by_mmsi(mmsi, date)
        else:
            self.delete_by_imo(imo, date)
   
    def delete_by_mmsi(self, mmsi, date):
        query = 'DELETE FROM {table_name} ' \
                'WHERE date = {val} ' \
                'AND mmsi = {val}'
        query = query.format(table_name=self.full_table_name, val=self.database.placeholder)
        self.database.cursor.execute(query, (date, mmsi))
    
    def delete_by_imo(self, imo, date):
        query = 'DELETE FROM {table_name} ' \
                'WHERE date = {val} ' \
                'AND imo = {val}'
        query = query.format(table_name=self.full_table_name, val=self.database.placeholder)
        self.database.cursor.execute(query, (date, imo))
        
    def clear_bad_data(self):
        query = 'DELETE FROM {table_name} WHERE date not like "2%"'
        query = query.format(table_name=self.full_table_name)
        self.database.cursor.execute(query)
        self.database.commit()

    def add_position(self, position_info):
        query = 'INSERT INTO {table_name} (mmsi, imo, date, latitude, longitude, speed) ' \
                'VALUES ({val},{val},{val},{val},{val},{val})'
        query = query.format(table_name=self.full_table_name, val=self.database.placeholder)
        mmsi = position_info['mmsi']
        imo = position_info['imo']
        date = position_info['date']
        latitude = position_info['latitude']
        longitude = position_info['longitude']
        speed = position_info['speed']
        self.database.cursor.execute(query, (mmsi, imo, date, latitude, longitude, speed))

    def batch_insert(self, ret):
        query = 'INSERT INTO {table_name} (idx, mmsi, imo, date, latitude, longitude, speed) ' \
                'VALUES {val}'
        query = query.format(table_name=self.full_table_name, val=self.database.placeholder)
        psycopg2.extras.execute_values(self.database.cursor, query, ret)

    def make_geometries_index(self):
        query = 'CREATE INDEX ON {table_name} USING gist(geom);'
        query = query.format(table_name=self.full_table_name)
        self.database.cursor.execute(query)
        self.database.commit()

    def make_geometries(self):
        query = 'UPDATE {table_name} SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) \
                WHERE geom IS NULL \
                AND latitude IS NOT NULL;'
        query = query.format(table_name=self.full_table_name)
        self.database.cursor.execute(query)
        self.database.commit()

    def create(self):
        query = 'CREATE TABLE {table_name} (' \
                'idx serial,' \
                'mmsi integer,' \
                'imo integer,' \
                'date timestamp without time zone,' \
                'latitude double precision,' \
                'longitude double precision,' \
                'speed double precision,' \
                'geom geometry);'
        query = query.format(table_name=self.full_table_name)
        self.database.cursor.execute(query)
        self.database.commit()
