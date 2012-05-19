import sys
import zmq
import simplejson as serializer
import time
from ctx import ctx
from gzip import GzipFile
from cStringIO import StringIO

ZMQ_PUBSUB_KV8 = "tcp://83.98.158.170:7817"
ZMQ_PUBSUB_KV8_ANNOTATE = "tcp://127.0.0.1:7818"

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
                fid = '_'.join([row['DataOwnerCode'],  row['LinePlanningNumber'], row['LocalServiceLevelCode'], row['JourneyNumber'], row['FortifyOrderNumber'], row['UserStopOrderNumber']])
                hours, minutes, seconds = row['ExpectedDepartureTime'].split(':')
                departure = ((int(hours) * 60) + int(minutes)) * 60 + int(seconds)
                actuals[fid] = departure

                #sys.stderr.write('\r%d' % (len(actuals)))
            print fid

    elif socks.get(client_annotate) == zmq.POLLIN:
        array = client_annotate.recv_json()
        # print array
        result = {}
        print '\n\n'
        for item in array:
            print item
            if item in actuals:
                result[item] = actuals[item]

        print '\n\n'

        client_annotate.send_json(result)

        #now = int(time.time())
        #for key, val in actuals.items():
        #    if val < now:
        #        del actuals[key]
