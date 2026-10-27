import json, os, re, time
from datetime import datetime, timezone

import msgpack
import zmq

BIND = os.environ.get("REFERENCE_BIND", "tcp://*:5559")
TIMEOUT = float(os.environ.get("HEARTBEAT_TIMEOUT", "30"))  # segundos sem heartbeat -> removido
NAME_RE = re.compile(r"^[A-Za-z0-9_-]{1,32}$")

ranks = {} # nome -> rank (nunca reaproveitado; mesmo nome => mesmo rank)
adresses = {} # nome -> endereço ZeroMQ do servidor (para comunicação entre servidores)
alive = {} # nome -> último sinal de vida (monotonic)
next_rank = 1


class LogicalClock:
    def __init__(self):
        self.value = 0

    def tick(self):                      # antes de cada envio
        self.value += 1
        return self.value

    def update(self, received):          # ao receber: máximo entre o recebido e o local
        self.value = max(self.value, received)
        return self.value


clock = LogicalClock()


def log(tag, msg):
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
    print(f"[{ts}] [reference] {tag} {json.dumps(msg, ensure_ascii=False)}", flush=True)


def reply(rtype, status="ok", **extra):
    ts = int(time.time() * 1000)
    # "time" = hora correta (usada pelos servidores no algoritmo de Cristian)
    return {"type": rtype, "timestamp": ts, "time": ts, "clock": clock.tick(),
            "status": status, **extra}


def error(rtype, code, message):
    return reply(rtype, "error", error=code, message=message)


def purge():
    now = time.monotonic()
    for name in [n for n, t in alive.items() if now - t > TIMEOUT]:
        del alive[name]
        log("REMOVIDO", {"name": name, "rank": ranks[name], "motivo": "sem heartbeat"})


def register(name, address=None):
    global next_rank
    if isinstance(address, str) and 0 < len(address) <= 200:
        adresses[name] = address
    if name not in ranks:
        ranks[name] = next_rank
        next_rank += 1
    alive[name] = time.monotonic()
    return ranks[name]


def server_list():
    return [{"name": n, "rank": ranks[n], "address": adresses.get(n)} for n in sorted(alive, key=ranks.get)]


def do_rank(req):
    name = req.get("name")
    if not isinstance(name, str) or not NAME_RE.match(name):
        return error("rank_reply", "invalid_name", "nome inválido")
    return reply("rank_reply", rank=register(name, req.get("address")))


def do_list(req):
    purge()
    return reply("list_reply", servers=server_list())


def do_heartbeat(req):
    name = req.get("name")
    if not isinstance(name, str) or not NAME_RE.match(name):
        return error("heartbeat_reply", "invalid_name", "nome inválido")
    return reply("heartbeat_reply", rank=register(name, req.get("address")))  # volta à lista se tinha sido removido


HANDLERS = {"rank": do_rank, "list": do_list, "heartbeat": do_heartbeat}


def handle(raw):
    try:
        req = msgpack.unpackb(raw, raw=False)
        assert isinstance(req, dict)
        assert isinstance(req.get("timestamp"), int) and isinstance(req.get("clock"), int)
    except Exception:
        return error("error", "invalid_message", "mensagem malformada (timestamp/clock obrigatórios)")
    clock.update(req["clock"])
    log("<< RECEBIDO", req)
    purge()
    handler = HANDLERS.get(req.get("type"))
    if handler is None:
        return error("error", "unknown_type", f"tipo desconhecido: {req.get('type')}")
    return handler(req)


def main():
    ctx = zmq.Context()
    sock = ctx.socket(zmq.REP)
    sock.bind(BIND)
    log("INICIADO", {"bind": BIND, "heartbeat_timeout_s": TIMEOUT})
    poller = zmq.Poller()
    poller.register(sock, zmq.POLLIN)
    while True:
        if sock in dict(poller.poll(1000)):
            resp = handle(sock.recv())
            log(">> ENVIADO", resp)
            sock.send(msgpack.packb(resp, use_bin_type=True))
        else:
            purge()


if __name__ == "__main__":
    main()
