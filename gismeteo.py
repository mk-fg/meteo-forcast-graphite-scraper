#!/usr/bin/env python
# -*- coding: utf-8 -*-


## URL grabbed from https://addons.mozilla.org/ru/firefox/addon/gismeteobar/
# See GISBar.GISClass.prototype.getFrc (and req.open there)
#  to see how such url is built and XML response is parsed.
# Gist is:
#  http://<this.host>/inform-service/<this.ishash>/forecast/?city=<city_id>&lang=ru
#  Here this.* parameters are defined on top in GISClass
#  city_id can be acquired from e.g. http://www.gismeteo.ru/city/daily/4517/ url (4517 here)
# Probably bad idea to run this for any kind of non-personal thing.


from __future__ import print_function
from time import sleep
import os, sys, requests, arrow, re, socket


gismeteo_url_base = ( 'http://d6a5c954.services.gismeteo.ru/'
	'inform-service/0f14315098daad405ded9270d1500bcb/'
	'forecast/?city={city_id}&lang=ru' )


def main(args=None):
	import argparse
	parser = argparse.ArgumentParser(
		description='Script to grab Gismeteo (gismeteo.ru service) forecast'
			' data for current time of the day and relay it to graphite carbon daemon.')
	parser.add_argument('carbon', metavar='carbon_ip',
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
	parser.add_argument('-d', '--debug', action='store_true', help='Verbose operation mode.')
	opts = parser.parse_args(sys.argv[1:] if args is None else args)

	global log
	import logging
	logging.basicConfig(level=logging.DEBUG if opts.debug else logging.WARNING)
	log = logging.getLogger()

	host, port = opts.carbon.split(':')
	port = int(port)

	## Fetch XML data
	url = gismeteo_url_base.format(city_id=opts.city_id)
	res = requests.get(url)
	res.raise_for_status()
	xml = res.content
	# with open('gismeteo_data') as src: xml = src.read()

	## Make sure schema is what it's expected to be and get values
	from lxml import etree
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
		match = re.search(r'^\d{4}-\d{2}-\d{2}T\d:\d{2}:\d{2}$', ts)
		if match: ts = ts[:11] + '0' + ts[11:]
		try: ts = arrow.get(ts)
		except:
			log.error('Failed to parse timestamp: %s', ts)
			raise
		return ts.replace(tzinfo=opts.timezone)

	fact_ts = parse_ts(fact.attrib['valid'])
	values = [('fact', float(fact_val.attrib['t']), fact_ts)]
	for fc, fc_val in zip(fcs, fcs_val):
		fc_ts = parse_ts(fc.attrib['valid'])
		delta = fc_ts - fact_ts
		assert delta.total_seconds() % (6 * 3600) == 0, [fc_ts, fact_ts, delta] # 6h increments
		offset = int(delta.total_seconds() // 3600)
		assert offset in valid_offsets, [fc_ts, fact_ts, offset]
		values.append(('h_{:03d}'.format(offset), float(fc_val.attrib['t']), fc_ts))

	## Connect to carbon TCP socket
	c, d_min, d_max, d_k = map(float, opts.reconnect.split(':'))
	assert c >= 0 and (d_k == 0 or d_k >= 1), [c, d_k]
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
			log.debug('Connected to Carbon at {}:{}'.format(*sa))
		except (socket.error, socket.gaierror) as err:
			if c <= 0: raise
			c -= 1
			if isinstance(err, socket.gaierror):
				log.info('Failed to resolve host ({!r}): {}'.format(host, err))
			else:
				log.info('Failed to connect to {}:{}: {}'.format(host, port, err))
			sleep(max(0, d))
			d = max(d_min, min(d_max, d * d_k))
		else: break

	## Send stuff
	for name, val, ts in values:
		if isinstance(ts, arrow.Arrow): ts = ts.float_timestamp
		line = '{}{} {} {}\n'.format(opts.metric_prefix or '', name, val, int(ts))
		log.debug('Sending line: {!r}'.format(line))
		sock.sendall(line)

	## Done
	sock.close()
	log.debug('Finished successfully')



if __name__ == '__main__': sys.exit(main())
