import pickle
import struct
import carbon_pb2

data = [
	[('param1', (1423931224, 60.2))],
	[('param1', (1423931224, 60.2), (1423931225, 50.2), (1423931226, 40.2))],
	[('param1', (1423931224, 60.2)), ('param2', (1423931224, -15))],
	[('param1', (1423931224, 60.2), (1423931284, 42)), ('param2', (1423931224, -15))],
]

for i, r in enumerate(data):
	msg = pickle.dumps(r)
	print "pickle #{0}: {1} (len={2})".format(i, repr(msg), len(msg))

for i, r in enumerate(data):
	payload = carbon_pb2.Payload()
	for m in r:
		payload.metrics.add()
		metric = payload.metrics[-1]
		metric.metric = m[0]
		for p in m[1:]:
			metric.points.add()
			point = metric.points[-1]
			point.timestamp = p[0]
			point.value = p[1]

	msg = payload.SerializeToString()
	print "protobuf #{0}: {1} (len={2})".format(i, repr(msg), len(msg))


listOfMetricTuples = [("hello.world", (1452200952, 42))]
payload = pickle.dumps(listOfMetricTuples, protocol=2)
header = struct.pack("!L", len(payload))
message = header + payload
print "pickle frame:", repr(message)

payload_data = carbon_pb2.Payload()
payload_data.metrics.add()
metric = payload_data.metrics[-1]
metric.metric = "hello.world"
metric.points.add()
point = metric.points[-1]
point.timestamp = 1452200952
point.value = 42
payload = payload_data.SerializeToString()
header = struct.pack("!L", len(payload))
message = header + payload
print "protobuf frame:", repr(message)