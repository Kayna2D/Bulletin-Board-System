import json, os, re, time
from datetime import datetime, timezone

import msgpack
import zmq

NAME = os.environ.get("SERVER_NAME", "server")
BROKER = os.environ.get("BROKER_BACKEND", "tcp://broker:5556")
PUB_PROXY = os.environ.get("PUB_PROXY", "tcp://proxy:5557") # XSUB
SUB_PROXY = os.environ.get("SUB_PROXY", "tcp://proxy:5558") # XPUB
REFERENCE = os.environ.get("REFERENCE", "tcp://reference:5559")
SKEW_MS = int(os.environ.get("CLOCK_SKEW_MS", "0")) # simula relógio físico errado
HEARTBEAT_EVERY = int(os.environ.get("HEARTBEAT_EVERY", "10")) # mensagens de clientes
DATA_DIR = os.environ.get("DATA_DIR", "/data")
DATA_FILE = os.path.join(DATA_DIR, "data.json") # logins + canais
MESSAGES_FILE = os.path.join(DATA_DIR, "messages.jsonl") # publicações (append-only)

USER_RE = re.compile(r"^[A-Za-z0-9_-]{1,32}$")
CHANNEL_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,31}$") # não começa com "_" (reservado)
SYNC_TOPIC = b"_sync" # tópico interno entre servidores
MAX_MSG_LEN = 1000

# relógio lógico 
class LogicalClock:
    def __init__(self):
        self.value = 0

    def tick(self): # incrementa antes de cada envio
        self.value += 1
        return self.value

    def update(self, received): # ao receber: máximo entre o recebido e o local
        self.value = max(self.value, received)
        return self.value


clock = LogicalClock()

# relógio físico (corrigido pelo serviço de referência) 
offset_ms = 0.0


def raw_now_ms():
    return time.time() * 1000 + SKEW_MS

def now_ms():
    return int(raw_now_ms() + offset_ms)

def adjust_clock(ref_time, t0, t1):
    """Algoritmo de Cristian: hora estimada = hora da referência + RTT/2."""
    global offset_ms
    rtt = t1 - t0
    offset_ms = (ref_time + rtt / 2) - t1
    log("RELÓGIO AJUSTADO", {"offset_ms": round(offset_ms, 2), "rtt_ms": round(rtt, 2)})

def log(tag, msg):
    ts = datetime.fromtimestamp(now_ms() / 1000, timezone.utc).strftime("%H:%M:%S.%f")[:-3]
    print(f"[{ts}] [{NAME}] {tag} {json.dumps(msg, ensure_ascii=False)}", flush=True)


# persistência (cada servidor tem os seus arquivos) 
def load():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, encoding="utf-8") as f:
            return json.load(f)
    return {"logins": [], "channels": []}


def save(db):
    os.makedirs(DATA_DIR, exist_ok=True)
    tmp = DATA_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(db, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, DATA_FILE)  # escrita atômica


def append_message(record):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(MESSAGES_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())


db = load()
rank = None

# sockets 
ctx = zmq.Context()
rep = ctx.socket(zmq.REP)
rep.connect(BROKER)
pub = ctx.socket(zmq.PUB)
pub.connect(PUB_PROXY)
sub = ctx.socket(zmq.SUB) # só para replicar canais entre servidores
sub.connect(SUB_PROXY)
sub.setsockopt(zmq.SUBSCRIBE, SYNC_TOPIC)
ref = None # REQ para o serviço de referência

# serviço de referência 
def connect_reference():
    global ref
    if ref is not None:
        ref.close(linger=0)
    ref = ctx.socket(zmq.REQ)
    ref.setsockopt(zmq.RCVTIMEO, 2000)
    ref.setsockopt(zmq.SNDTIMEO, 2000)
    ref.connect(REFERENCE)


def ask_reference(rtype, **extra):
    """Envia REQ à referência; retorna a resposta (dict) ou None se falhar."""
    req = {"type": rtype, "timestamp": now_ms(), "clock": clock.tick(), "name": NAME, **extra}
    log(">> REF ENVIADO", req)
    try:
        t0 = raw_now_ms()
        ref.send(msgpack.packb(req, use_bin_type=True))
        raw = ref.recv()
        t1 = raw_now_ms()
    except zmq.ZMQError:
        log("!! REF sem resposta", {"type": rtype})
        connect_reference() # REQ travado após timeout: recria o socket
        return None
    resp = msgpack.unpackb(raw, raw=False)
    clock.update(resp.get("clock", 0))
    log("<< REF RECEBIDO", resp)
    if resp.get("status") == "ok" and isinstance(resp.get("time"), int):
        adjust_clock(resp["time"], t0, t1)
    return resp


def register():
    """Pede o rank (e já acerta o relógio); repete até a referência responder."""
    global rank
    while True:
        resp = ask_reference("rank")
        if resp and resp.get("status") == "ok":
            rank = resp["rank"]
            log("REGISTRADO", {"name": NAME, "rank": rank})
            return
        time.sleep(2)


def heartbeat():
    """Heartbeat + hora correta; depois atualiza a lista de servidores."""
    global rank
    resp = ask_reference("heartbeat")
    if resp and resp.get("status") == "ok" and resp.get("rank") != rank:
        rank = resp["rank"]
        log("RANK ATUALIZADO", {"rank": rank})
    lst = ask_reference("list")
    if lst and lst.get("status") == "ok":
        log("SERVIDORES DISPONÍVEIS", lst["servers"])

# respostas 
def reply(rtype, status="ok", **extra):
    return {"type": rtype, "timestamp": now_ms(),
            "clock": clock.tick(),
            "status": status, "server": NAME, **extra}


def error(rtype, code, message):
    return reply(rtype, "error", error=code, message=message)


# handlers 
def do_login(req):
    user = req.get("user")
    if not isinstance(user, str) or not USER_RE.match(user):
        return error("login_reply", "invalid_username",
                     "nome deve ter 1-32 chars [A-Za-z0-9_-]")
    db["logins"].append({"user": user, "timestamp": req["timestamp"], "clock": req["clock"]})
    save(db)
    return reply("login_reply", message=f"bem-vindo(a) {user}")


def do_channel(req):
    name = req.get("channel")
    if not isinstance(name, str) or not CHANNEL_RE.match(name):
        return error("channel_reply", "invalid_channel",
                     "canal deve ter 1-32 chars [A-Za-z0-9_-] e não começar com '_'")
    if name in db["channels"]:
        return error("channel_reply", "channel_exists", f"canal '{name}' já existe")
    db["channels"].append(name)
    save(db)
    # avisa os outros servidores para que todos conheçam o canal
    pub.send_multipart([SYNC_TOPIC, msgpack.packb(
        {"type": "channel_sync", "timestamp": now_ms(), "clock": clock.tick(),
         "origin": NAME, "channel": name}, use_bin_type=True)])
    return reply("channel_reply", message=f"canal '{name}' criado")


def do_channels(req):
    return reply("channels_reply", channels=list(db["channels"]))


def do_publish(req):
    channel, text = req.get("channel"), req.get("message")
    if not isinstance(channel, str) or not CHANNEL_RE.match(channel):
        return error("publish_reply", "invalid_channel", "nome de canal inválido")
    if channel not in db["channels"]:
        return error("publish_reply", "channel_not_found", f"canal '{channel}' não existe")
    if not isinstance(text, str) or not text.strip() or len(text) > MAX_MSG_LEN:
        return error("publish_reply", "invalid_message",
                     f"mensagem deve ter 1-{MAX_MSG_LEN} caracteres")
    publication = {"type": "publication", "timestamp": now_ms(),
                   "clock": clock.tick(), "server": NAME, 
                   "channel": channel, "user": req.get("user"), 
                   "message": text}
    # persiste antes de publicar: nada que foi publicado fica sem registro
    append_message({**publication, "request_timestamp": req["timestamp"], "request_clock": req["clock"]})
    pub.send_multipart([channel.encode("utf-8"),
                        msgpack.packb(publication, use_bin_type=True)])
    log("PUBLICADO", publication)
    return reply("publish_reply", message=f"publicado em '{channel}'")


HANDLERS = {"login": do_login, "channel": do_channel,
            "channels": do_channels, "publish": do_publish}


def handle(raw):
    try:
        req = msgpack.unpackb(raw, raw=False)
        assert isinstance(req, dict)
        assert isinstance(req, dict) and isinstance(req.get("timestamp"), int)
    except Exception:
        return error("error", "invalid_message", "mensagem malformada (timestamp/clock obrigatórios)")
    clock.update(req["clock"])
    log("<< RECEBIDO", req)
    handler = HANDLERS.get(req.get("type"))
    if handler is None:
        return error("error", "unknown_type", f"tipo desconhecido: {req.get('type')}")
    return handler(req)


def handle_sync(frames):
    try:
        data = msgpack.unpackb(frames[1], raw=False)
        channel = data["channel"]
        clock.update(data["clock"])
    except Exception:
        return
    if (data.get("origin") != NAME and isinstance(channel, str)
            and CHANNEL_RE.match(channel) and channel not in db["channels"]):
        db["channels"].append(channel)
        save(db)
        log("<< SYNC", data)


def main():
    time.sleep(1) # dá tempo das conexões PUB/SUB se estabelecerem (slow joiner)
    connect_reference()
    register()
    log("INICIADO", {"rank": rank, "canais": db["channels"]})
    poller = zmq.Poller()
    poller.register(rep, zmq.POLLIN)
    poller.register(sub, zmq.POLLIN)
    handled = 0
    while True:
        events = dict(poller.poll())
        if sub in events:
            handle_sync(sub.recv_multipart())
        if rep in events:
            resp = handle(rep.recv())
            log(">> ENVIADO", resp)
            rep.send(msgpack.packb(resp, use_bin_type=True))
            handled += 1
            if handled % HEARTBEAT_EVERY == 0: # a cada 10 mensagens de clientes
                log("HEARTBEAT", {"handled": handled})


if __name__ == "__main__":
    main()
