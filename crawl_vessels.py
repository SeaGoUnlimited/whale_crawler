from webpages import VesselPage
import database
import time
import pandas

sl_db = database.SQLiteDatabase('vessels.sqlite')
sl_position_table = database.PositionTable(sl_db)
sl_vessel_table = database.VesselTable(sl_db)

pg_db = database.PostgresDatabase('database.config', 'whale_watch')
pg_position_table = database.PositionTable(pg_db, schema='public')
pg_vessel_table = database.VesselTable(pg_db, schema='public')

vessel_page = VesselPage()
ship_urls = ['https://www.vesselfinder.com/vessels/CONDOR-EXPRESS-IMO-0-MMSI-367568350']


def get_ship(url):
    success = vessel_page.download(url)    
    in_database = vessel_page.in_database()
    if success and in_database:        
        vessel_page.to_file()        
        vessel_page.parse()
        vessel_params = vessel_page.vessel_params
        print('Vessel Name: {} - url: {}'.format(vessel_params['name'], vessel_params['url']))
        upsert_data(vessel_params)
        

def upsert_data(vessel_params):
    sl_position_table.upsert_position(vessel_params)
    sl_position_table.commit()
    sl_vessel_table.upsert_vessel(vessel_params)
    sl_vessel_table.commit()
    pg_position_table.upsert_position(vessel_params)
    pg_position_table.commit()
    pg_vessel_table.upsert_vessel(vessel_params)
    pg_vessel_table.commit()
    df = pandas.DataFrame(vessel_params, index=['mmsi'])
    with open('positions.csv', 'a') as csv:
      df.to_csv(path_or_buf=csv, header=False)


if __name__ == '__main__':
  while True:
    for ship_url in ship_urls:
      get_ship(ship_url)
    time.sleep(600)
    

