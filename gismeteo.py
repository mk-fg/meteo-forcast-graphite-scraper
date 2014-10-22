#!/usr/bin/env python
# -*- coding: utf-8 -*-


from __future__ import print_function
from time import sleep
from lxml import etree, html
import os, sys, requests, arrow, re, socket, types, unicodedata


DIGIT = dict( MINUS=u'-', ZERO=u'0', ONE=u'1', TWO=u'2', THREE=u'3',
	FOUR=u'4', FIVE=u'5', SIX=u'6', SEVEN=u'7', EIGHT=u'8', NINE=u'9', STOP=u'.' )

def digit_guess(unistr):
	return u''.join( v for u in unistr
		for k,v in DIGIT.viewitems() if k in unicodedata.name(u) )


def send(host, port, reconnect, prefix, values):
	# "values" should be list of tuples of "name, value, timestamp"

	## Connect to carbon TCP socket
	c, d_min, d_max, d_k = reconnect
	d = d_min
	while True:
		try:
			try:
				addrinfo = list(reversed(socket.getaddrinfo(
					host, port, socket.AF_UNSPEC, socket.SOCK_STREAM )))
			except socket.error as err:
				raise socket.gaierror(err.message)
			assert addrinfo, addrinfo
			while addrinfo:
				# Try connecting to all of the returned addresses
				af, socktype, proto, canonname, sa = addrinfo.pop()
				try:
					sock = socket.socket(af, socktype, proto)
					sock.connect(sa)
				except socket.error:
					if not addrinfo: raise
			log.debug('Connected to Carbon at %s', sa)
		except (socket.error, socket.gaierror) as err:
			if c <= 0: raise
			c -= 1
			if isinstance(err, socket.gaierror):
				log.info('Failed to resolve host (%r): %s', host, err)
			else:
				log.info('Failed to connect to %s:%s: %s', host, port, err)
			sleep(max(0, d))
			d = max(d_min, min(d_max, d * d_k))
		else: break

	## Send stuff
	for name, val, ts in values:
		if isinstance(ts, arrow.Arrow): ts = ts.float_timestamp
		line = '{}{} {} {}\n'.format(prefix or '', name, val, int(ts))
		log.debug('Sending line: %r', line)
		sock.sendall(line)

	## Done
	sock.close()



## URL grabbed from https://addons.mozilla.org/ru/firefox/addon/gismeteobar/
# See GISBar.GISClass.prototype.getFrc (and req.open there)
#  to see how such url is built and XML response is parsed.
# Gist is:
#  http://<this.host>/inform-service/<this.ishash>/forecast/?city=<city_id>&lang=ru
#  Here this.* parameters are defined on top in GISClass
#  city_id can be acquired from e.g. http://www.gismeteo.ru/city/daily/4517/ url (4517 here)
# Probably bad idea to run this for any kind of non-personal thing.

gismeteo_url_base = ( 'http://d6a5c954.services.gismeteo.ru/'
	'inform-service/0f14315098daad405ded9270d1500bcb/'
	'forecast/?city={city_id}&lang=ru' )

def scrape_shortterm(city_id, tz='local', data_path=None):
	## Fetch XML data
	if not data_path:
		log.debug('Fetching short-term data...')
		url = gismeteo_url_base.format(city_id=city_id)
		res = requests.get(url)
		res.raise_for_status()
		xml = res.content
	else:
		log.debug('Reading short-term data...')
		with open(data_path) as src: xml = src.read()

	## Make sure schema is what it's expected to be and get values
	log.debug('Processing data...')
	t = etree.fromstring(xml)

	def dump(el=t): return etree.tostring(el, encoding='utf-8')
	def get_els(el, xpath, count=1):
		els = el.xpath(xpath) if not isinstance(el, list)\
			else [el.xpath(xpath) for el in el]
		assert len(els) == count, [dump(el), xpath, els, count]
		return els
	el, = get_els(t, '/weather')
	el, = get_els(el, './location')
	fact, = get_els(el, './fact')
	fact_val, = get_els(fact, './values')
	fcs = get_els(el, './forecast', 4)
	fcs_val = [el[0] for el in get_els(fcs, './values', 4)]

	## Gismeteo has 4 "Time of Day" slots in a day: 3am, 9am, 3pm, 9pm
	valid_tod = 3, 9, 15, 21
	valid_offsets = 6, 12, 18, 24

	def parse_ts(ts):
		# Well done on fucking-up iso8601 timestamps generation, gismeteo
		if isinstance(ts, types.StringTypes):
			match = re.search(r'^\d{4}-\d{2}-\d{2}T\d:\d{2}:\d{2}$', ts)
			if match: ts = ts[:11] + '0' + ts[11:]
		try: ts = arrow.get(ts)
		except:
			log.error('Failed to parse timestamp: %s', ts)
			raise
		return ts.replace(tzinfo=tz)

	day = parse_ts(fact.attrib['valid']).floor('day')
	fact_tod = int(fact.attrib['tod'])
	if fact_tod < 0: fact_tod = 0 # how the fuck they parse their own data... no idea!
	fact_ts = day.replace(hour=valid_tod[fact_tod])

	values = [('fact', float(fact_val.attrib['t']), fact_ts)]
	for fc, fc_val in zip(fcs, fcs_val):
		fc_tod = int(fc.attrib['tod'])
		fc_ts = day.replace(hour=valid_tod[fc_tod])
		if fc_tod <= fact_tod: fc_ts = fc_ts.replace(days=1)
		delta = fc_ts - fact_ts
		assert delta.total_seconds() % (6 * 3600) == 0, [fc_ts, fact_ts, delta]
		offset = int(delta.total_seconds() // 3600)
		assert offset in valid_offsets, [fc_ts, fact_ts, offset, (fc_tod, fact_tod)]
		values.append(('h_{:03d}'.format(offset), float(fc_val.attrib['t']), fc_ts))

	return values


## e1.ru is a local mirror of gismeteo data

def scrape_longterm(values_chk, tz='local', data_path=None):
	if not data_path:
		log.debug('Fetching long-term data...')
		res = requests.get('http://pogoda.e1.ru/m')
		res.raise_for_status()
		res = res.text
	else:
		log.debug('Reading long-term data...')
		with open(data_path) as src: res = src.read().decode('utf-8')

	log.debug('Processing data...')
	try: res = html.fromstring(res)
	except etree.XMLSyntaxError, etree.ParserError:
		from lxml.html.soupparser import fromstring as soup
		res = soup(html)

	rows = res.xpath('//table[@class="weather-table"]/tr')
	assert rows and len(rows) % 5 == 0, len(rows)
	rows = iter(rows)
	values = list()

	# Same rules as for gismeteo, but with extra checks on text data values
	valid_tod = [3, 9, 15, 21]
	dow_dict = [ u'понедельник', u'вторник',\
		u'среда', u'четверг', u'пятница', u'суббота', u'воскресенье' ]
	month_dict = [ u'январ', u'феврал', u'март', u'апрел',\
		[u'май', u'мая'], u'июн', u'июл', u'август', u'сентябр', u'октябр', u'ноябр' , u'декабр' ]
	tod_dict = [u'ночь', u'утро', u'день', u'вечер']

	## Re-calculate current "tod" (0-4) and "fact_ts", as passed to graphite, for deltas
	for n, temp_chk, fact_ts in values_chk:
		if n == 'fact': break
	else: raise ValueError(values_chk)
	fact_tod = valid_tod.index(fact_ts.hour)
	day = fact_ts.replace(tzinfo=tz).floor('day')
	val_chk_idx = dict() # for checks vs values_chk

	for n, header in enumerate(rows):
		date = day.replace(days=n)

		# Make sure it's for correct day
		text, = header.xpath('./th/h3[@class="weather-table-header"]/text()')
		text = text.lower()
		text_day, = re.findall(r'\d+', text)
		for text_month, mo in enumerate(month_dict, 1):
			if isinstance(mo, types.StringTypes): mo = [mo]
			for mo in mo:
				if mo in text: break
			else: continue
			break
		else: raise ValueError('month', text)
		for text_dow, dow in enumerate(dow_dict):
			if dow in text: break
		else: raise ValueError('dow', text)
		date_chk = date.format('DD.MM.{}').format(date.weekday())
		date_text = '{:02d}.{:02d}.{}'.format(int(text_day), text_month, text_dow)
		if date_chk != date_text: # page rotates slightly before midnight
			date = date.replace(days=1)
			date_chk = date.format('DD.MM.{}').format(date.weekday())
		assert date_chk == date_text, [date_chk, date_text]

		# Next 4 rows contain all 4 tod data in order
		for fc_tod in xrange(4):
			row = next(rows)
			assert row.attrib['class'] == 'weather-table-row{}'.format(fc_tod+1), row.attrib['class']
			tod_text, = row.xpath('./td[@class="weather-table-key"]/text()')
			assert fc_tod == tod_dict.index(tod_text.lower()), [fc_tod, tod_text]
			temp, = row.xpath('./td[@class="weather-table-temp"]/p/text()')
			assert u'°' in temp, repr(temp)
			temp = temp.strip().split(u'°', 1)[0]
			temp = temp.replace(u'\u2212', u'-') # py bug #6632
			try: temp = float(temp)
			except UnicodeEncodeError:
				temp = float(digit_guess(temp))
			fc_ts = date.replace(hour=valid_tod[fc_tod])
			delta = fc_ts - fact_ts
			assert delta.total_seconds() % (6 * 3600) == 0, [fc_ts, fact_ts, delta]
			offset = int(delta.total_seconds() // 3600) # most will be more than one day
			if offset < 6: continue # past data
			n = 'h_{:03d}'.format(offset)
			val = n, temp, fc_ts
			values.append(val)
			val_chk_idx[n] = val

	## Remove values that are duplicate with short-term ones, make sure they match
	for n, temp_chk, ts in values_chk:
		if n not in val_chk_idx: continue
		val = val_chk_idx[n]
		n, temp, fc_ts = val
		# gismeteo and e1 values can differ by a fuckton, surprisingly
		assert abs(temp - temp_chk) <= 10 and ts == fc_ts, [val, temp_chk, ts]
		values.pop(values.index(val))

	return values



def main(args=None):
	import argparse
	parser = argparse.ArgumentParser(
		description='Script to grab Gismeteo (gismeteo.ru service) forecast'
			' data for current time of the day and relay it to graphite carbon daemon.')
	parser.add_argument('carbon', metavar='carbon_ip:port',
		help='Carbon daemon IP address and port of TCP linereader socket.')
	parser.add_argument('-c', '--city-id',
		metavar='gismeteo_city_id', type=int, required=True,
		help='Gismeteo ID of the city to grab data/forecast for. Can be acquired from gismeteo URLs.')
	parser.add_argument('-t', '--timezone', metavar='arrow_timezone', default='local',
		help='Arrow (pypi module) timezone string'
				' to interpret gismeteo timestamps as (default: %(default)s).'
			' Should probably be local timezone of a city, specified in --city-id.')
	parser.add_argument('-p', '--metric-prefix', metavar='prefix_string',
		help='Prefix string to use for graphite metric (must have trailing dot, if necessary).')
	parser.add_argument('-r', '--reconnect',
		metavar='count:delay_min:delay_max:delay_k', default='100:1:60:2',
		help='Parameters for re-trying connection'
				' to carbon daemon if initial attempt(s) fail (floats, default: %(default)s).'
			' Only applied to initial connection attempt'
				' - if data cannot be sent after that, script fails.')
	parser.add_argument('--data-shortterm', metavar='path',
		help='Path to pre-fetched data dump to process for short-term forecast.')
	parser.add_argument('--data-longterm', metavar='path',
		help='Path to pre-fetched data dump to process for long-term forecast.')
	parser.add_argument('--debug', action='store_true', help='Verbose operation mode.')
	opts = parser.parse_args(sys.argv[1:] if args is None else args)

	global log
	import logging
	logging.basicConfig(level=logging.DEBUG if opts.debug else logging.WARNING)
	log = logging.getLogger()

	host, port = opts.carbon.split(':')
	port = int(port)
	c, d_min, d_max, d_k = map(float, opts.reconnect.split(':'))
	assert c >= 0 and (d_k == 0 or d_k >= 1), [c, d_k]
	reconnect = c, d_min, d_max, d_k

	values = list()
	values.extend(scrape_shortterm(opts.city_id, tz=opts.timezone, data_path=opts.data_shortterm))
	values.extend(scrape_longterm(values, tz=opts.timezone, data_path=opts.data_longterm))

	log.debug('Sending data...')
	send(host, port, reconnect, opts.metric_prefix, values)
	log.debug('Finished successfully')


if __name__ == '__main__': sys.exit(main())
