import pickle
import struct
import carbon_pb2

data = [
	[('param1', (1423931224, 60.2))],
	[('param1', (1423931224, 60.2), (1423931225, 50.2), (1423931226, 40.2))],
	[('param1', (1423931224, 60.2)), ('param2', (1423931224, -15))],
	[('param1', (1423931224, 60.2), (1423931284, 42)), ('param2', (1423931224, -15))],
]

def encode_pickle(msg):
	return pickle.dumps(msg)

def encode_protobuf(msg):
	payload = carbon_pb2.Payload()
	for m in msg:
		payload.metrics.add()
		metric = payload.metrics[-1]
		metric.metric = m[0]
		for p in m[1:]:
			metric.points.add()
			point = metric.points[-1]
			point.timestamp = p[0]
			point.value = p[1]

	return payload.SerializeToString()

def encode_plain(msg):
	out = ""
	for m in msg:
		for p in m[1:]:
			out += "%s %s %s\n" % (m[0], p[1], p[0])
	return out

for i, r in enumerate(data):
	s = encode_pickle(r)
	print "pickle #{0}: {1} (len={2})".format(i, repr(s), len(s))

for i, r in enumerate(data):
	s = encode_protobuf(r)
	print "protobuf #{0}: {1} (len={2})".format(i, repr(s), len(s))

for i, r in enumerate(data):
	s = encode_plain(r)
	print "plain #{0}: {1} (len={2})".format(i, repr(s), len(s))


print "\n-- \n"


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


print "\n-- \n"

bench100 = []
for i in xrange(100):
	bench100.append((
		"carbon.agents.graph1.tcp.metricReceived{0}".format(i),
		(1423931224, 42.2+i),
	))

print "bench100 pickle:", repr(encode_pickle(bench100))
print "bench100 protobuf:", repr(encode_protobuf(bench100))
print "bench100 plain:", repr(encode_plain(bench100))
