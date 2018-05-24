from bs4 import BeautifulSoup
import datetime
import requests
import time


def sign_coordinate(coordinate):
    if 'W' in coordinate or 'S' in coordinate:
        direction = -1
        coordinate = coordinate.rstrip('S')
        coordinate = coordinate.rstrip('W')
    elif 'E' in coordinate or 'N' in coordinate:
        direction = 1
        coordinate = coordinate.rstrip('N')
        coordinate = coordinate.replace('E', '')
    try:
        coordinate = float(coordinate) * direction
    except ValueError:
        coordinate = None
    return coordinate


class Webpage:
    def __init__(self):
        self.soup = None
        self.html = None
        self.table1 = None

    def to_file(self):
        with open('page.html', 'w') as outfile:
            outfile.write(self.html)

    def from_file(self):
        with open('page.html', 'r') as infile:
            self.soup = BeautifulSoup(infile.read(), 'lxml')


class VesselPage(Webpage):
    def __init__(self):
        super(VesselPage, self).__init__()
        self.vessel_params = dict()
        self.url = None

    def download(self, url):
        self.vessel_params['url'] = url
        session = requests.Session()
        session.headers.update({'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:53.0) Gecko/20100101 Firefox/53.0'})
        download_success = False
        while download_success == False:
            try:
                ret = session.get(url=url)
                self.html = ret.text
                download_success = True
            except requests.exceptions.ConnectionError:
                print('Download Failed. Retrying in 5 Seconds')
                time.sleep(5)

        self.soup = BeautifulSoup(self.html, 'lxml')
        if ret.status_code == 200:
            return True
        else:
            return False

    def parse(self):
        self.get_name()
        self.get_report_date()
        self.get_type()
        self.get_country()
        self.get_location()
        self.get_speed()
        self.get_imo()
        self.get_mmsi()
        self.get_built_year()
        self.get_length()
        self.get_width()
        self.get_gt()
        
    def in_database(self):
        in_database = True
        error_tags = self.soup.find_all('p', 'col-md-8')
        if len(error_tags) > 0:
            error_string = error_tags[0].contents[0]
            if 'temporary not in our database' in error_string:
                in_database = False
        return in_database            

    def get_name(self):
        name = self.soup.find('title').contents[0].split(' - ')[0]
        self.vessel_params['name'] = name

    def get_report_date(self):
        date_tag = self.soup.find(text='Last report')
        if date_tag is not None:
            date_string = date_tag.parent.next_sibling.contents[1].strip()
            try:
                string_format = '%b %d, %Y %H:%M UTC'
                timestamp = datetime.datetime.strptime(date_string, string_format)
            except:
                print('could not extract timestamp from {}'.format(date_string))
                timestamp = None
        else:
            timestamp = None
        self.vessel_params['date'] = timestamp        

    def get_type(self):
        ship_type_tag = self.soup.find(text='AIS Type')
        if ship_type_tag is None:
            ship_type_tag = self.soup.find(text='Ship type')
        ship_type = ship_type_tag.parent.next_sibling.contents[0].strip()
        self.vessel_params['ship_type'] = ship_type

    def get_country(self):
        country_tags = self.soup.find_all(text='Flag')
        if len(country_tags[0].parent.next_sibling.contents) > 0:
            country = country_tags[0].parent.next_sibling.contents[0].strip()
        elif len(country_tags[1].parent.next_sibling.contents) > 0:
            country = country_tags[1].parent.next_sibling.contents[0].strip()
        else:
            print('could not find country')
            country = None
        self.vessel_params['country'] = country

    def get_location(self):
        coordiantes_tag = self.soup.find(text='Coordinates')
        if coordiantes_tag is not None:
            coordiantes_string = coordiantes_tag.parent.next_sibling.contents[0].strip()
            latitude = coordiantes_string.split('/')[0]
            longitude = coordiantes_string.split('/')[1]
            latitude = sign_coordinate(latitude)
            longitude = sign_coordinate(longitude)
        else:
            latitude = None
            longitude = None
        self.vessel_params['latitude'] = latitude
        self.vessel_params['longitude'] = longitude

    def get_speed(self):
        speed_tag = self.soup.find(text='Course / Speed')
        if speed_tag is not None:
            speed_string = speed_tag.parent.next_sibling.contents[0].strip()
            speed = speed_string.split('/')[1].split('kn')[0].strip()
            try:
                speed = float(speed)
            except ValueError:
                print('could not extract speed from {}'.format(speed_string))
                speed = None
        else:
            speed = None
        self.vessel_params['speed'] = speed

    def get_imo(self):
        if self.soup.find(text='IMO / MMSI') is not None:
            imo_tag = self.soup.find(text='IMO / MMSI')
            imo_string = imo_tag.parent.next_sibling.contents[0].strip()
            imo = imo_string.split('/')[0].strip()
        elif self.soup.find(text='IMO number') is not None:
            imo_tag = self.soup.find(text='IMO number')
            imo = imo_tag.parent.next_sibling.contents[0].strip()
        try:
            imo = int(imo)
        except ValueError:
            print('could not extract imo from {}'.format(imo_string))
            imo = None
        self.vessel_params['imo'] = imo

    def get_mmsi(self):
        if self.soup.find(text='IMO / MMSI') is not None:
            mmsi_tag = self.soup.find(text='IMO / MMSI')
            mmsi_string = mmsi_tag.parent.next_sibling.contents[0].strip()
            mmsi = mmsi_string.split('/')[1].strip()
            try:
                mmsi = int(mmsi)
            except ValueError:
                print('could not extract mmsi from {}'.format(mmsi_string))
                mmsi = None
        else:
            mmsi = None
        self.vessel_params['mmsi'] = mmsi

    def get_built_year(self):
        built_string = self.soup.find(text='Year of Built').parent.next_sibling.contents[0].strip()
        try:
            built = int(built_string)
        except ValueError:
            print('could not extract built year from {}'.format(built_string))
            built = None
        self.vessel_params['built'] = built

    def get_length(self):
        if self.soup.find(text='Length / Beam') is not None:
            length_tag = self.soup.find(text='Length / Beam')
            length_string = length_tag.parent.next_sibling.contents[0].strip()
            length_string = length_string.split('/')[0].strip()
        elif self.soup.find(text='Length Overall (m)') is not None:
            length_tag = self.soup.find(text='Length Overall (m)')
            length_string = length_tag.parent.next_sibling.contents[0].strip()
        try:
            length = float(length_string)
        except ValueError:
            length = None
            print('could not extract length from {}'.format(length_string))
        except AttributeError:
            length = None
            print('could not extract length from {}'.format(length_string))
        self.vessel_params['length'] = length

    def get_width(self):
        if self.soup.find(text='Length / Beam') is not None:
            width_tag = self.soup.find(text='Length / Beam')
            width_string = width_tag.parent.next_sibling.contents[0].strip()
            if len(width_string.split('/')) > 1:
                width_string = width_string.split('/')[1].strip().split(' ')[0]
            else:
                width_string = None
        elif self.soup.find(text='Beam (m)') is not None:
            width_tag = self.soup.find(text='Beam (m)')
            width_string = width_tag.parent.next_sibling.contents[0].strip()
        try:
            width = float(width_string)
        except (IndexError, ValueError, AttributeError, TypeError) as e:
            print('could not extract width from {}'.format(width_string))
            width = None
        self.vessel_params['width'] = width

    def get_gt(self):
        gt_string = self.soup.find(text='Gross Tonnage').parent.next_sibling.contents[0].strip()
        try:
            gt = float(gt_string)
        except ValueError:
            gt = None
            print('could not extract gt from {}'.format(gt_string))
        self.vessel_params['gt'] = gt

