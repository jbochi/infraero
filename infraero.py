# -*- coding: utf-8 -*-
from BeautifulSoup import BeautifulSoup
try:
    import cPickle as pickle
except ImportError:
    import pickle

from cookielib import CookieJar
import datetime
import gzip
import re
from StringIO import StringIO
import time
import urllib
import urllib2

icao_codes = ['SBAR', 'SBBE', 'SBCF', 'SBBV', 'SBBR', 'SBCG', 'SBCY', 'SBCT', 'SBFL', 'SBFZ', 'SBGO',
              'SBJP', 'SBMQ', 'SBMO', 'SBEG', 'SBNT', 'SBPA', 'SBPV', 'SBRF', 'SBRB', 'SBGL', 'SBRJ',
              'SBSV', 'SBSL', 'SBSP', 'SBGR', 'SBKP', 'SBTE', 'SBUL', 'SBVT', 'SBCR', 'SBKG', 'SBFI',
              'SBLO', 'SBBH', 'SBPJ', 'SBPL', 'SBHT', 'SBCP', 'SBCM', 'SBCJ', 'SBCZ', 'SBIZ', 'SBIL',
              'SBJU', 'SBJV', 'SBMA', 'SBME', 'SBMK', 'SBNF', 'SBPK', 'SBSJ', 'SBSN', 'SBUR', 'SBUG']

class StringCookieJar(CookieJar):
    def __init__(self, string=None, policy=None):
        CookieJar.__init__(self, policy)
        if string:
            self._cookies = pickle.loads(string)

    def dump(self):
        return pickle.dumps(self._cookies)

class InfraeroError(Exception):
    pass

class Infraero:
    BASE_URL = 'http://www.infraero.gov.br/voos/'
    HOME_URL = BASE_URL + 'index.aspx'
    RESULTS_URL = BASE_URL + 'index_2.aspx'
    _PAGE_PATTERN = re.compile('Page\$(\d+)')
    _FLIGHT_DATA_PATTERN = re.compile('^grd_voos_ctl')
    _CONTROL_PATTERN = re.compile('grd_voos_ctl(\d{2})_(.*)')
    _GRID_CONTROLS = {'nom_cia': 'company',
                      'num_voo': 'flight_number',
                      'nom_localidade': 'airport',
                      'SIG_UF': 'UF',
                      'dat_voo': 'date',
                      'hor_prev': 'estimate',
                      'HOR_CONF': 'confirmed',
                      'lbl_escala': 'stops',
                      'DSC_STATUS': 'status'}
    _form_data = {}

    def __init__(self, state=None, proxy=None, max_retries=3):
        """ Classe para fazer scrap do status dos voos da Infraero

        args:
        @state: Estado de scrapper anterior obtido via .get_state()        
        @proxy: Proxy HTTP
        """
        self.max_retries = max_retries

        if state:
            self._form_data = state['form_data']
            self._cj = StringCookieJar(state['cookies'])
        else:
            self._cj = StringCookieJar()

        cookie_handler = urllib2.HTTPCookieProcessor(self._cj)
        if proxy is None:
            self._opener = urllib2.build_opener(cookie_handler)
        else:
            proxy_handler = urllib2.ProxyHandler({'http': proxy, })
            self._opener = urllib2.build_opener(cookie_handler, proxy_handler)

    def _url_open(self, url, data=None, delta=False, referer=HOME_URL):
        encoded_data = urllib.urlencode(data) if data else None
        default_headers = {'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-GB; rv:1.9.2.9) Gecko/20100824 Firefox/3.6.9 ( .NET CLR 3.5.30729; .NET4.0E)',
                           'Accept-Language': 'pt-br;q=0.5',
                           'Accept-Charset': 'utf-8;q=0.7,*;q=0.7',
                           'Accept-Encoding': 'gzip',
                           'Connection': 'close',
                           'Cache-Control': 'no-cache',
                           'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                           'Referer': referer,}

        req = urllib2.Request(url, encoded_data, default_headers, origin_req_host=referer)

        if delta:
            req.add_header('X-MicrosoftAjax', 'Delta=true')

        retries = 0
        try:
            handle = self._opener.open(req)
        except urllib2.HTTPError:
            retries += 1
            if retries > self.max_retries:
                raise
        
        if handle.info().get('Content-Encoding') == 'gzip':
            data = handle.read()
            buf = StringIO(data)
            f = gzip.GzipFile(fileobj=buf)
            response = f.read()
        else:
            response = handle.read()

        html = response.decode('utf-8')
        validate = url == self.RESULTS_URL
        self._form_data = self._get_input_data_from_html(html, validate=validate)

        return html

    def _open_home(self):
        """Open home page for search"""
        return self._url_open(self.HOME_URL)

    def _open_results(self):
        """Open search results"""
        return self._url_open(self.RESULTS_URL)

    def _post_search_form(self, airport):
        data = self._form_data
        data['btnPesquisar'] = 'Consultar Voos'
        data['aero_companias_aeroportos'] = airport
        return self._url_open(self.HOME_URL, data, delta=True)

    def _set_completed_on(self, html):
        data = self._form_data
        data['ScriptManager1'] = 'Update_Finalizados|chkFinalizados'
        data['__EVENTTARGET'] = 'chkFinalizados'
        data['chkFinalizados'] = 'on'

        return self._url_open(self.RESULTS_URL, data, delta=False)

    def _set_departure(self, html):
        data = self._form_data
        data['ScriptManager1'] = 'update_grid|lnk_partidas'
        data['__EVENTTARGET'] = 'lnk_partidas'
        return self._url_open(self.RESULTS_URL, data, delta=False)

    def _get_page(self, page):
        data = self._form_data
        data['ScriptManager1'] = 'update_grid|grd_voos'
        data['__EVENTTARGET'] = 'grd_voos'
        data['__EVENTARGUMENT'] = 'Page$%d' % page
        return self._url_open(self.RESULTS_URL, data, delta=False)

    def _get_input_data_from_html(self, html, button=None, validate=True):
        soup = BeautifulSoup(html)

        error_style = 'font-family:arial;font-size:10;color:red'
        if validate and soup.find('span', attrs={'style': error_style}):
            raise InfraeroError
        
        tags = soup.findAll('input')

        data = {}
        for tag in tags:
            attrs = dict(tag.attrs)
            if attrs.get('type') != 'submit' or attrs['id'] == button:
                data[attrs['id']] = attrs.get('value', '')

        return data

    def _parse_flight_tag(self, tag):
        """Returns a tuple (flight, key, data)"""
        flight, infraero_ctl = self._CONTROL_PATTERN.match(tag['id']).groups()
        key = self._GRID_CONTROLS[infraero_ctl]
        if key == 'stops':
            # List of tuples (IATA_CODE, Name)
            stops = tag.findAll(text=True)
            data = [stop.split(' - ') for stop in stops]
        elif tag.string:
            # Other data treated as text
            data = tag.string.strip()
            if key == 'estimate' or key == 'confirmed':
                # Convert to time
                data = datetime.time(*[int(d) for d in data.split(':')])
            elif key == 'date':
                day, month = [int(d) for d in data.split('/')]
                data = datetime.date(datetime.datetime.now().year, month, day)
        else:
            data = None

        return (flight, key, data)

    def _parse_flights(self, soup):
        tags = soup.findAll(['span'], {'id': self._FLIGHT_DATA_PATTERN})
        flights = {}

        for tag in tags:
            flight, key, data = self._parse_flight_tag(tag)
            if flight not in flights:
                flights[flight] = {key: data}
            else:
                flights[flight][key] = data

        return flights.values()

    def _parse_page_from_tag(self, page_tag):
        if page_tag.string != '...':
            return int(page_tag.string)
        else:
            href = page_tag['href']
            return int(self._PAGE_PATTERN.search(href).groups()[0])

    def _parse_pages(self, soup):
        tr = soup.find('tr', {'class': 'pagina'})
        if tr is None:
            return {'current': 1,
                    'continue': False,
                    'pages': [1]}

        page_tags = tr.findAll(['span', 'a'])

        current_page = self._parse_page_from_tag(tr.find('span'))
        pages = [self._parse_page_from_tag(tag) for tag in page_tags]
        pages_continue = page_tags[-1].string == '...'

        return {'current': current_page,
                'continue': pages_continue,
                'pages': pages}

    def _parse_date(self, soup):
        time_tag = soup.find('span', {'id': 'lbl_data_criacao'})
        if time_tag:
            time_string = time_tag.string[:17]
            time_format = '%d/%m/%y %H:%M:%S'
            time_struct = time.strptime(time_string, time_format)
            timestamp = time.mktime(time_struct)
            return datetime.datetime.fromtimestamp(timestamp)

    def _parse_html(self, html):
        soup = BeautifulSoup(html)
        return {'flights': self._parse_flights(soup),
                'date': self._parse_date(soup),
                'pages': self._parse_pages(soup)}

    def get_state(self):
        return {'cookies': self._cj.dump(),
                'form_data': self._form_data}

    def search_airport(self, airport, completed=False, departure=False):
        self._open_home()
        self._post_search_form(airport)
        html = self._open_results()

        if departure:
            html = self._set_departure(html)

        if completed:
            html = self._set_completed_on(html)

        return self._parse_html(html)

    def change_page(self, page):
        html = self._get_page(page)
        return self._parse_html(html)



###################################3

def test():
    results = {}
    airports = set()
    stops = {}

    i = Infraero() #proxy='10.138.15.10:8080'
    for icao in icao_codes:
        print 'fetching', icao
        results[icao] = {}
        r = i.search_airport(icao, completed=False, departure=False)

        while True:
            page = r['pages']['current']
            results[icao][page] = r

            for flight in r['flights']:
                if flight['airport'] not in airports:
                    airports.add((flight['airport'], flight['UF']))
                for stop in flight['stops']:
                    stops[stop[0]] = stop[1]

            if page + 1 in r['pages']['pages']:
                print 'fetching', icao, page + 1
                r = i.change_page(r['pages']['current'] + 1)
            else:
                break

    return (airports, stops)
