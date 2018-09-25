#!/usr/bin/python
#-*-coding:utf-8-*-


import geoip2.database

def ip_parse(ip):
	reader = geoip2.database.Reader('/home/ubuntu12/GroupEvent/group_event/group_event/ip/GeoLite2-City/GeoLite2-City.mmdb')
	try:
		response = reader.city(ip)
	except:
		parse_ip = "unknown"
	try:
		state = response.continent.names["zh-CN"].encode('utf-8')
	except:
		state = 'unknown'
	try:
		country = response.country.names["zh-CN"].encode('utf-8')
	except:
		country = 'unknown'
	try:
		province = response.subdivisions.most_specific.names["zh-CN"].encode('utf-8')
	except:
		province = 'unknown'
	try:
		city = response.city.names["zh-CN"].encode('utf-8') 
	except:
		city = 'unknown'

	parse_ip =  state + '&' + country  + '&'+  province + '&'+ city

	return parse_ip


if __name__ == '__main__':
	aa = ip_parse('73.231.33.243')
	print aa





