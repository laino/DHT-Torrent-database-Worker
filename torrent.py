from thread import start_new_thread
import lightdht
import struct
import time
import socket as pysocket
import bencode
import traceback
import math
import random
import hashlib

#Timeouts

#Exceeding one of these will resilu in the peer being disconnected
CONNECT_TIMEOUT = 10 #Maximum amount of time to wait for a connection to be established
RECV_TIMEOUT = 60 #Maximum amount of time to wait for recv to get all the data
PEER_TIMEOUT = 30 #Maximum amount of time to wait for a block of metadata
#A torrent that takes longer than this will be canceled
TORRENT_TIMEOUT = 600

def recvAll(stream, l, timeout = RECV_TIMEOUT):
	data = ""
	start = time.time()
	while True:
		if time.time() >= start + timeout:
			raise PeerException, "Read timed out"
		try:
			data = data + stream.recv(l - len(data))
		except pysocket.timeout:
			pass
		if len(data) < l:
			time.sleep(0.5)
		else:
			break
	return data

class PeerException(Exception):
	pass

class Peer:
	def __init__(self, socket, torrent = None, timeout = PEER_TIMEOUT):
		socket.setblocking(True)
		socket.settimeout(RECV_TIMEOUT)
		self.timeout = timeout
		self.socket = socket
		self.torrent = torrent
		self.handshakeSend = False
		self.handshakeReceived = False
		self.extensionHandshakeReceived = False
		self.closed = False

	def _receiveHandshake(self):
		pstr_len = ord(recvAll(self.socket,1))
		pstr = recvAll(self.socket, pstr_len) 
		if pstr != "BitTorrent protocol":
			self.close()
			raise PeerException, "Peer uses wrong protocol" 
		
		self.reserved = recvAll(self.socket,8)
		#Check if the peer supports the extension protocol
		if ord(self.reserved[5]) & 0x10 != 0x10:
			self.socket.close()
			raise PeerException, "Not supporting extensions"

		self.info_hash = recvAll(self.socket,20)
		self.peer_id = recvAll(self.socket,20)
		self.handshakeReceived = True

	def _sendMessage(self, msgtype = None, contents = None):
		l = 0
		msg = ""
		if msgtype != None:
			l = l + 1
			msg = msg + chr(msgtype)
		if contents != None:
			l = l + len(contents)
			msg = msg + contents
		packed = struct.pack(">I",l) + msg
		self.socket.sendall(packed)

	#Returns tuple(length, msgtype, data)
	def _receiveMessage(self):
		socket = self.socket
		length = struct.unpack(">I",recvAll(socket,4))[0]
		msgtype = None
		content = None
		if length>0:
			msgtype = ord(recvAll(socket,1))
			if length>1:
				content = recvAll(socket,length-1)

		return (length, msgtype, content)
	
	def _sendHandshake(self):
		#Send the handshake
		#           1 byte         8 byte      20byte     20byte
		#handshake: <pstrlen><pstr><reserved><info_hash><peer_id>
		pstr = "BitTorrent protocol"
		pstr_len = len(pstr)
		reserved = [chr(0) for i in range(8)]
		reserved[5] = chr(0x10)
		reserved = ''.join(reserved)
		info_hash = self.torrent.info_hash
		_id = "-TI0002-TORRENTINDEX"
		packed = chr(pstr_len) + pstr + reserved + info_hash + _id
		self.socket.sendall(packed)
		self._sendExtensionHandshake()
		self.handshakeSend = True

	def _sendExtensionHandshake(self):
		contents = {'m': {'ut_metadata': 3}, 'metadata_size':0,'v':'DHT-Crawler-0.1'}	
		self._sendExtensionMessage(0, contents)		
	
	def _sendExtensionMessage(self, msg, contents, add = None):
		data = chr(msg) + bencode.bencode(contents) 
		if add != None:
			data = data + add		
		self._sendMessage(20, data)

	def doReceiveHandshake(self):
		if not self.handshakeReceived:
			self._receiveHandshake()

	def performHandshake(self):
		"""
		Performs a complete handshake with the peer
		"""
		while not self.handshakeSend or not self.handshakeReceived:
			if not self.handshakeSend and self.torrent != None:
				self._sendHandshake()
			if not self.handshakeReceived:
				self._receiveHandshake()
			time.sleep(0.1)
	

	def _requestPiece(self):
		if self.torrent.finished:
			return
		piece = self.torrent.getNeededPiece()
		self._sendExtensionMessage(self.metadata_id,{'msg_type':0,'piece':piece})		

	def resetTimeout(self):
		self.limit = time.time() + self.timeout

	#Mainloop
	def loop(self):
		self.resetTimeout()
		while not self.torrent.finished and not self.closed:
			if time.time() >= self.limit:
				raise PeerException, "Peer timed out"
			length, msgtype, content = self._receiveMessage()
			if length > 0:
				if msgtype == 20:
					#extended
					self._extended(content)
				elif msgtype == 0:
					#Choke
					pass
				elif msgtype == 1:
					#unchoke
					pass
				elif msgtype == 2:
					#interested
					pass
				elif msgtype == 3:
					#not interested
					pass
				elif msgtype == 4:
					#have
					pass
	
	def _metadataExt(self, msg, extra):
		msg_type = msg['msg_type']
		torrent = self.torrent
		if msg_type == 0:
			#request
			#currently we are rejeting all of them
			piece = msg['piece']
			self.sendExtensionMessage(self.metadata_id,{'msg_type':2,'piece':piece})				
		elif msg_type == 1:
			#data
			self.resetTimeout()
			size = msg['total_size']
			if size != self.torrent.metadataSize:
				raise PeerException, "Peer was reporting wrong metadata size during download"
			piece = msg['piece']	
			self.torrent.gotMetadata(piece, extra)	
			self._requestPiece()
		elif msg_type == 2:
			#reject
			self.close()
			raise PeerException, "Peer is rejecting metadata requests"

	def _extended(self, data):
		msgtype = ord(data[0])
		if msgtype == 0 and not self.extensionHandshakeReceived:
			#handshake
			payload = bencode.bdecode(data[1:])
			if not "metadata_size" in payload or not "ut_metadata" in payload['m']:
				self.close()
				raise PeerException, "Not supporting ut_metadata extension"
			
			size = payload['metadata_size']
			if size == 0:
				self.close()
				raise PeerException, "The peer does not appear to have any metadata"

			self.torrent.setMetadataSize(size)
			self.metadata_id = payload['m']['ut_metadata']
			self.extensionHandshakeReceived = True
			#everything seems fine, go ahead an request the first bit of metadata
			self._requestPiece()
			self.resetTimeout()
		elif not self.extensionHandshakeReceived:
			self.close()
			raise PeerException, "Peer send extension messages before handshake"
		
		if msgtype == 3:
			#Got metadata extension message
			r, l = bencode.bdecode_len(data[1:])
			self._metadataExt(r, data[l+1:])

	def close(self):
		self.socket.close()
		self.closed = True

class Torrent:
	def __init__(self, dht, info_hash, get_metadata):
		self.get_metadata = get_metadata
		self.dht = dht
		self.info_hash = info_hash
		self.metadata = {}
		self.metadataSize = -1
		self.metadataPieces = 0
		self.finished = False
		self.peer_list = set()
		self.peers = []
		self.started = time.time()
		self.shutdown = False
		self.got_peers = False 
		start_new_thread(self._run, tuple())
	
	def gotMetadata(self, piece, content):
		length = len(content)
		slength = 16384
		if piece == self.metadataPieces -1:
			slength = self.metadataSize % 16384
		if length < slength :
			raise PeerException, "Received metadata piece of wrong length ("+str(length)+"/"+str(slength)+")"
		elif length > slength:
			content = content[0:slength]
		if not piece in self.metadata:
			self.metadata[piece] = content
		#Check if the torrent is finished
		if self.getNeededPiece() == -1:
			self.finished = True
	
	def peerCount(self):
		return len(self.peer_list)

	def disconnect(self):
		self.shutdown = True
		for peer in self.peers:
			try:
				peer.close()
			except Exception, e:
				print(str(e))
				traceback.print_exc()	
			finally:
				try:
					self.peers.remove(peer)
				except ValueError:
					#Was not in list
					pass

	def setMetadataSize(self, size):
		if size == 0:
			raise PeerException, "Metadata size cannot be 0"
		self.metadataSize = size
		self.metadataPieces = int(math.ceil(size / 16384.0))
		self.log("Downloading "+str(self.metadataPieces)+" pieces of metadata ("+str(size)+" bytes)")
	
	def getNeededPiece(self):
		"""
		Returns a random metadata piece we still need
		"""
		piece = 0
		pieces = []
		while piece < self.metadataPieces:
			if not piece in self.metadata:
				pieces.append(piece)
			piece += 1
		if len(pieces) == 0:
			return -1
		return random.choice(pieces)

	def openConnection(self, ip, port):
		socket = pysocket.create_connection((ip, port), CONNECT_TIMEOUT)
		peer = Peer(socket, self)
		peer.performHandshake()
		self._handlePeer(peer)

	def addPeer(self, peer):
		if not self.get_metadata:
			peer.close()
			raise Exception, "Not interested in metadata"
		peer.torrent = self
		peer.performHandshake()
		self._handlePeer(peer)

	def _handlePeer(self, peer):
		if peer.info_hash != self.info_hash:
			peer.close()
			raise PeerException, "Peer is serving the wrong torrent"
		self.peers.append(peer)
		try:
			peer.loop()
		finally:
			peer.close()
			try:
				self.peers.remove(peer)
			except ValueError:
				#was not on list
				pass
	
	def _updatePeers(self):
		peer_list = None
		try:
			peer_list = self.dht.get_peers(self.info_hash)
		except (lightdht.KRPCTimeout, lightdht.KRPCError, lightdht.NotFoundError), e:
			self.log("Problem getting peer list: "+str(e))
			return

		if peer_list == None:
			return
		if type(peer_list) == str:
			peer_list = [peer_list]
		peer_list = filter(lambda x:len(x)==6, peer_list)
		self.got_peers = True
		self.peer_list = set(list(self.peer_list) + peer_list)

	def _run(self):

		tries = 0
		while not self.finished and not self.shutdown and tries <2:
			if tries != 0:
				time.sleep(10)
			tries += 1
			self._updatePeers()
			
			if not self.get_metadata:
				if len(self.peer_list) > 0:
					break
				else:
					continue

			for peer in self.peer_list:
				if self.finished or self.shutdown:
					break
				if len(peer)!=6:
					continue
				data = struct.unpack('>BBBBH',peer)
				ip = '.'.join([str(d) for d in data[:4]])
				port = data[4]
				try:
					self.openConnection(ip, port)					
				except (PeerException, pysocket.error, pysocket.timeout), e:
					self.log("Error "+ip+": "+str(e))
					#traceback.print_exc()
		self.finished = True

	def prepareData(self):
		if not self.get_metadata:
			return None
		num = len(self.metadata)
		if num != self.metadataPieces or num == 0:
			return None
		data = ""
		for i in range(num):
			data = data + self.metadata[i]
		sha = hashlib.sha1()
		sha.update(data)
		info_hash = sha.digest()
		if info_hash != self.info_hash:
			self.log("The hashes do not match! ("+info_hash.encode("hex")+") ")
			return None
		return data		

	def log(self, what):
		print "Torrent "+(self.info_hash.encode("hex"))+": "+str(what)

class TorrentManager:
	def __init__(self, dht, port, onfinish, timeout = TORRENT_TIMEOUT):
		self.timeout = timeout
		self.dht = dht
		self.port = port
		self.onfinish = onfinish
		self.running = {}
		#Don't handle incoming connections
		#start_new_thread(self._run,tuple())

	def addTorrent(self, info_hash, metadata = True):
		if not info_hash in self.running:
			torrent = Torrent(self.dht, info_hash, metadata)
			self.running[info_hash] = torrent
	
	def count(self):
		return len(self.running)

	def fetchAndRemove(self):
		now = time.time()
		ret = []
		for info_hash in self.running.keys():
			torrent = self.running[info_hash]
			if torrent.finished:
				del self.running[info_hash]
				torrent.disconnect()
				data = torrent.prepareData()
				peers = torrent.peerCount()
				if data != None or torrent.got_peers:
					ret.append((info_hash, peers, data))
			elif now > torrent.started + self.timeout:
				del self.running[info_hash]
				torrent.log("Timeout")
				torrent.disconnect()
		return ret

	def _run(self):
		serversocket = pysocket.socket(pysocket.AF_INET, pysocket.SOCK_STREAM)
		serversocket.bind(('localhost', self.port))
		serversocket.listen(10)
		while True:
			socket, address = serversocket.accept()
			start_new_thread(self._handlePeer, tuple(socket))

	def _handlePeer(self, socket):
		try:
			peer = Peer(socket)
			peer.doReceiveHandshake()
			info_hash = peer.info_hash			
			if info_hash in self.running:
				torrent = self.running[info_hash]
				torrent.addPeer(peer)
			else:
				peer.close()
		except Exception, e:
			print "Error while handling incoming connection: "+str(e)
