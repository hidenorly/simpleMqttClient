#!/usr/bin/env python
# coding: utf-8
#
# Copyright (C) 2017 hidenorly
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import paho.mqtt.client as mqtt
from optparse import OptionParser, OptionValueError

class MQTTSubscriber:
	def __init__(self, topic):
		self.topic = topic
		self.wildCard = False
		if topic.endswith("#"):
			self.wildCard = True
			self._topic = topic[0:len(topic)-1]

	def onMessage(self, msg):
		print(msg.topic + " " + str(msg.payload))

	def canHandle(self, topic):
		if self.topic == topic or (self.wildCard==True and topic.startswith(self._topic)):
			return True
		return False

class MQTTManager:
	def __init__(self, clientId, server, port, username, password, bSecure=False):
		self.server = server
		self.port = port
		self.username = username
		self.password = password
		self.bSecure = bSecure
		self.subscribers = {}
		self.clientId = clientId
		self.client = mqtt.Client(client_id=clientId, clean_session=True, protocol=mqtt.MQTTv311, userdata=self)
		self.client.on_connect = self.onConnect
		self.client.on_message = self.onMessage

	def __del__(self):
		self.disconnect()

	def addSubscriber(self, key, aSubscriber):
		self.subscribers[key] = aSubscriber

	def publish(self, topic, val, qos=0, retain=0):
		self.client.publish(topic, val, qos, retain)

	def setTls(self, ca_certs, certfile=None, keyfile=None, cert_reqs=mqtt.ssl.CERT_REQUIRED, tls_version=mqtt.ssl.PROTOCOL_TLSv1, ciphers=None):
		self.ca_certs = ca_certs
		self.certfile = certfile
		self.keyfile = keyfile
		self.cert_reqs = cert_reqs
		self.tls_version = tls_version
		self.ciphers = ciphers
		self.bSecure = True

	def connect(self):
		if self.bSecure:
			self.client.tls_set(self.ca_certs, self.certfile, self.keyfile, self.cert_reqs, self.tls_version, self.ciphers)
		self.client.connect(self.server, port=self.port, keepalive=60)

	def disconnect(self):
		self.client.disconnect()

	def loop(self):
		self.client.loop_forever()

	def enableSubscriber(self, key, bEnable):
		if self.subscribers.has_key(key):
			if bEnable:
				self.client.subscribe( self.subscribers[key].topic )
			else:
				self.client.unsubscribe( self.subscribers[key].topic )

	@staticmethod
	def onConnect(client, pSelf, flags, result):
		print("Connected with result code " + str(result))

	@staticmethod
	def onMessage(client, pSelf, msg):
		for topic, aSubscriber in pSelf.subscribers.iteritems():
			if aSubscriber.canHandle(msg.topic):
				aSubscriber.onMessage(msg)

class MySubscriber(MQTTSubscriber):
	def onMessage(self, msg):
		print("MySubscriber:" + msg.topic + " " + str(msg.payload))

if __name__ == '__main__':
	parser = OptionParser()

	parser.add_option("-c", "--clientId", action="store", type="string", dest="clientId", help="Specify client Id", default="simpleMqttClient")
	parser.add_option("-s", "--host", action="store", type="string", dest="host", help="Specify mqtt server")
	parser.add_option("-p", "--port", action="store", type="int", dest="port", help="Specify mqtt port", default=1883)
	parser.add_option("-u", "--username", action="store", type="string", dest="username", help="Specify username", default=None)
	parser.add_option("-k", "--password", action="store", type="string", dest="password", help="Specify password", default=None)
	parser.add_option("-t", "--topic", action="store", type="string", dest="topic", help="Specify subscribing topic", default="#")
	parser.add_option("-v", "--publishVal", action="store", type="string", dest="publishVal", help="Specify publishing value (use with --topic)", default=None)

	(options, args) = parser.parse_args()

	mqtt = MQTTManager(options.clientId, options.host, options.port, options.username, options.password, False)

	mqtt.connect()

	if options.publishVal:
		mqtt.publish(options.topic, options.publishVal)
	else:
		aSubscriber = MySubscriber(options.topic)
		mqtt.addSubscriber( aSubscriber.topic, aSubscriber )
		mqtt.enableSubscriber( aSubscriber.topic, True )
		mqtt.loop()

	mqtt.disconnect()
