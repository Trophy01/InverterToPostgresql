#!/usr/bin/env python3
"""
Send request commands to a battery device over MQTT.

- Builds proper protocol headers (start code 0x4350, version 0x11)
- Sends batPropertyExtReq, batPropertyReq, batPositionReq
- Prints the exact topic and hex payload sent
- Optionally repeats N cycles with a delay between requests

Usage examples:
  python send_battery_requests.py --device 862317043581508
  python send_battery_requests.py --device 862317043581508 --cycles 5 --delay 1.0 -v
"""

import argparse
import logging
import sys
import time
import struct
import paho.mqtt.client as mqtt

START_CODE = 0x4350
PROTOCOL_VERSION = 0x11
PRODUCT = 'SW_GPS'


def build_header(seq: int, txn: int) -> bytes:
	return struct.pack('!H B H B', START_CODE, PROTOCOL_VERSION, seq & 0xFFFF, txn & 0xFF)


class BatteryRequester:
	def __init__(self, broker: str, port: int, device_id: str, cycles: int, delay_s: float, verbose: bool):
		self.broker = broker
		self.port = port
		self.device_id = device_id
		self.cycles = cycles
		self.delay_s = delay_s
		self.seq = 0
		self.txn = 0
		self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
		self.client.on_connect = self.on_connect
		self.client.on_message = self.on_message
		self.logger = logging.getLogger(__name__)
		level = logging.DEBUG if verbose else logging.INFO
		logging.basicConfig(level=level, format='%(asctime)s - %(levelname)s - %(message)s')

	def next_header(self) -> bytes:
		self.seq = (self.seq + 1) & 0xFFFF
		self.txn = (self.txn + 1) & 0xFF
		return build_header(self.seq, self.txn)

	def on_connect(self, client, userdata, flags, reason_code, properties=None):
		if reason_code == 0:
			self.logger.info('Connected to MQTT broker')
			# Subscribe to responses of interest for visibility
			for t in (
				f'/{PRODUCT}/{self.device_id}/user/batPropertyExtRprt',
				f'/{PRODUCT}/{self.device_id}/user/batPropertyExtRsp',
				f'/{PRODUCT}/{self.device_id}/user/batPropertyRprt',
				f'/{PRODUCT}/{self.device_id}/user/batPositonRprt',
				f'/{PRODUCT}/{self.device_id}/user/batPositionRprt',
			):
				client.subscribe(t, qos=1)
				self.logger.info(f'Subscribed: {t}')
		else:
			self.logger.error(f'MQTT connect failed: {reason_code}')

	def on_message(self, client, userdata, msg):
		# Best-effort hex print and response summary
		try:
			payload = msg.payload
			message_type = msg.topic.rsplit('/', 1)[-1]
			hex_str = None
			try:
				# Sometimes payloads are JSON with hex field
				text = payload.decode('utf-8')
				if text.strip().startswith('{'):
					import json
					obj = json.loads(text)
					hex_str = (obj.get('payload') or '').strip()
					if not hex_str:
						hex_str = payload.hex()
				else:
					hex_str = text.strip()
			except Exception:
				hex_str = payload.hex()
			self.logger.info(f'INCOMING | topic={msg.topic} | hex={hex_str}')
			# Decode header if possible
			try:
				raw = bytes.fromhex(hex_str) if hex_str and all(c in '0123456789abcdefABCDEF' for c in hex_str.replace(' ', '')) else payload
				if len(raw) >= 6:
					start_code, proto_ver, seq, txn = struct.unpack('!H B H B', raw[:6])
					self.logger.info(f"RESPONSE | type={message_type} | start=0x{start_code:04X} ver=0x{proto_ver:02X} seq={seq} txn={txn}")
			except Exception:
				pass
		except Exception as e:
			self.logger.debug(f'on_message error: {e}')

	def publish(self, subtopic: str, body: bytes = b''):
		hdr = self.next_header()
		topic = f'/{PRODUCT}/{self.device_id}/user/{subtopic}'
		payload = hdr + body
		res = self.client.publish(topic, payload=payload, qos=1, retain=False)
		hex_payload = payload.hex()
		self.logger.info(f'OUTGOING | topic={topic} | hex={hex_payload} | seq={self.seq} txn={self.txn}')
		return res.is_published()

	def run(self):
		self.client.connect(self.broker, self.port, 60)
		self.client.loop_start()
		try:
			for i in range(self.cycles):
				self.publish('batPropertyExtReq')
				time.sleep(self.delay_s)
				self.publish('batPropertyReq')
				time.sleep(self.delay_s)
				self.publish('batPositionReq')
				time.sleep(self.delay_s)
		finally:
			# allow a brief window to receive responses
			time.sleep(2.0)
			self.client.loop_stop()
			self.client.disconnect()


def parse_args() -> argparse.Namespace:
	p = argparse.ArgumentParser(description='Send request commands to a battery device')
	p.add_argument('--broker', default='mqtt-cloud-1.telco.co.zw', help='MQTT broker host')
	p.add_argument('--port', type=int, default=1883, help='MQTT broker port')
	p.add_argument('--device', default='862317043581508', help='Target device ID')
	p.add_argument('--cycles', type=int, default=3, help='Number of request cycles to send')
	p.add_argument('--delay', type=float, default=0.5, help='Delay between requests (seconds)')
	p.add_argument('-v', '--verbose', action='store_true', help='Verbose logging')
	return p.parse_args()


def main() -> int:
	args = parse_args()
	req = BatteryRequester(
		broker=args.broker,
		port=args.port,
		device_id=args.device,
		cycles=args.cycles,
		delay_s=args.delay,
		verbose=args.verbose,
	)
	req.run()
	return 0


if __name__ == '__main__':
	sys.exit(main())
