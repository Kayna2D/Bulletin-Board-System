import zmq

ctx = zmq.Context()
xsub = ctx.socket(zmq.XSUB)
xsub.bind("tcp://*:5557")
xpub = ctx.socket(zmq.XPUB)
xpub.bind("tcp://*:5558")
print("[proxy] XSUB :5557 | XPUB :5558", flush=True)
zmq.proxy(xsub, xpub)