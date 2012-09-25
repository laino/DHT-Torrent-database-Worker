import logging
import hashlib
import time
import os
import lightdht
import torrent
import json
import urllib
import urllib2
import traceback
import bencode
import random

#Maximal simultanous jobs
MAX_JOBS = 30
API_URL = "http://localhost:8080/mapi/";
API_PASS = "test"

# Enable logging:
lightdht.logger.setLevel(logging.ERROR)	 
formatter = logging.Formatter("[%(levelname)s@%(created)s] %(message)s")
stdout_handler = logging.StreamHandler()
stdout_handler.setFormatter(formatter)
lightdht.logger.addHandler(stdout_handler)

# Create a DHT node.
dht = lightdht.DHT(port=8000) 

#Running torrents that are downloading metadata
manager = torrent.TorrentManager(dht, 8000, None)
found_torrents = set()

def addHash(info_hash):
	if len(info_hash) == 20:
		found_torrents.add(info_hash)

def makeRequest(method, body = None):
	data = {'method':method, 'password':API_PASS}
	if body != None:
		body = bencode.bencode(body).encode("base64")
		data['body'] = body
	data = urllib.urlencode(data)
	while True:
		try:
			req = urllib2.Request(API_URL,data)
			response = urllib2.urlopen(req).read()
			return bencode.bdecode(response.decode("base64"))
		except (urllib2.HTTPError, urllib2.URLError), e:
			print "Error while making requests: %s. Retrying in 10 seconds" % str(e)
			time.sleep(10)
	return None

def sendFound():
	if len(found_torrents) == 0:
		return
	to_send = list()
	while len(found_torrents) != 0:
		to_send.append(found_torrents.pop())
	print("Sending %d info_hashes to server" % len(to_send))
	makeRequest('put_hashes',to_send)

def sendFinished():
	ret = manager.fetchAndRemove()
	for torrent in ret:
		info_hash, peers, data = torrent
		processFinished(info_hash, peers, data)

def getNewWork():
	njobs = manager.count()
	if njobs < MAX_JOBS:
		jobs =  get_work(MAX_JOBS - njobs)
		for work in jobs:
			if work['type'] == 'download_metadata':
				manager.addTorrent(work['info_hash'])
			elif work['type'] == 'check_peers':
				manager.addTorrent(work['info_hash'], metadata = False)

def processFinished(info_hash, peers, data):
	req = {'info_hash':info_hash, 'peers':peers}
	if data != None:
		req['metadata'] = data
	print "Sending info of %s" % info_hash.encode("hex")
	makeRequest('update',req)		

def get_work(amount):
	jobs = []
	for i in range(amount):
		jobs.append(makeRequest('get_work'))
	return jobs

# handler
def myhandler(rec, c):
	try:
		if "a" in rec:
			a = rec["a"]
			if "info_hash" in a:
				info_hash = a["info_hash"]
				addHash(info_hash)
	finally:
		dht.default_handler(rec,c) 

dht.handler = myhandler
dht.active_discovery = True 
dht.self_find_delay = 30

# Start it!
with dht:
	print "Started"
	# Go to sleep and let the DHT service requests.
	while True:
		sendFound()
		sendFinished()
		getNewWork()
		time.sleep(10)	
