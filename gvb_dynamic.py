from consts import ZMQ_PUBSUB_KV8, ZMQ_PUBSUB_KV8_ANNOTATE

import sys
import zmq
import simplejson as serializer
import time
from ctx import ctx
from gzip import GzipFile
from cStringIO import StringIO

# Initialize a zeromq CONTEXT
context = zmq.Context()
sys.stderr.write('Setting up a ZeroMQ SUB: %s\n' % (ZMQ_PUBSUB_KV8))
subscribe_kv8 = context.socket(zmq.SUB)
subscribe_kv8.connect(ZMQ_PUBSUB_KV8)
subscribe_kv8.setsockopt(zmq.SUBSCRIBE, '/GOVI/KV8')

sys.stderr.write('Setting up a ZeroMQ REP: %s\n' % (ZMQ_PUBSUB_KV8_ANNOTATE))
client_annotate = context.socket(zmq.REP)
client_annotate.bind(ZMQ_PUBSUB_KV8_ANNOTATE)

# Set up a poller
poller = zmq.Poller()
poller.register(subscribe_kv8, zmq.POLLIN)
poller.register(client_annotate, zmq.POLLIN)

# Cache
actuals = {}
while True:
    socks = dict(poller.poll())

    if socks.get(subscribe_kv8) == zmq.POLLIN:
        multipart = subscribe_kv8.recv_multipart()
        content = GzipFile('','r',0,StringIO(''.join(multipart[1:]))).read()
        c = ctx(content)
        if 'DATEDPASSTIME' in c.ctx:
            for row in c.ctx['DATEDPASSTIME'].rows():
		if row['DataOwnerCode'] != 'GVB':
			continue
                # fid = '|'.join([row['DataOwnerCode'],  row['LocalServiceLevelCode'], row['LinePlanningNumber'], row['JourneyNumber'], row['FortifyOrderNumber']]) + '_' + row['UserStopOrderNumber']
                fid = row['JourneyNumber'] + '_' + row['UserStopOrderNumber']
                hours, minutes, seconds = row['ExpectedDepartureTime'].split(':')
                departure = ((int(hours) * 60) + int(minutes)) * 60 + int(seconds)
                actuals[fid] = departure

                #sys.stderr.write('\r%d' % (len(actuals)))

    elif socks.get(client_annotate) == zmq.POLLIN:
        lookup = client_annotate.recv_json()
        result = {}
        for item in lookup:
            item = item.split('|')[-1]
            if item in actuals:
                print item
                result[item] = actuals[item]

        client_annotate.send_json(result)

        #now = int(time.time())
        #for key, val in actuals.items():
        #    if val < now:
        #        del actuals[key]
