import json, os, re, time
from datetime import datetime, timezone

import msgpack
import zmq

NAME = os.environ.get("SERVER_NAME", "server")
BROKER = os.environ.get("BROKER_BACKEND", "tcp://broker:5556")
PUB_PROXY = os.environ.get("PUB_PROXY", "tcp://proxy:5557")   # XSUB
SUB_PROXY = os.environ.get("SUB_PROXY", "tcp://proxy:5558")   # XPUB
DATA_DIR = os.environ.get("DATA_DIR", "/data")
DATA_FILE = os.path.join(DATA_DIR, "data.json")               # logins + canais
MESSAGES_FILE = os.path.join(DATA_DIR, "messages.jsonl")      # publicações (append-only)

USER_RE = re.compile(r"^[A-Za-z0-9_-]{1,32}$")
CHANNEL_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,31}$")  # não começa com "_" (reservado)
SYNC_TOPIC = b"_sync"                                         # tópico interno entre servidores
MAX_MSG_LEN = 1000


def now_ms():
    return int(time.time() * 1000)


def log(tag, msg):
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
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

# sockets 
ctx = zmq.Context()
rep = ctx.socket(zmq.REP)
rep.connect(BROKER)
pub = ctx.socket(zmq.PUB)
pub.connect(PUB_PROXY)
sub = ctx.socket(zmq.SUB)           # só para replicar canais entre servidores
sub.connect(SUB_PROXY)
sub.setsockopt(zmq.SUBSCRIBE, SYNC_TOPIC)


# respostas 
def reply(rtype, status="ok", **extra):
    return {"type": rtype, "timestamp": now_ms(),
            "status": status, "server": NAME, **extra}


def error(rtype, code, message):
    return reply(rtype, "error", error=code, message=message)


# handlers 
def do_login(req):
    user = req.get("user")
    if not isinstance(user, str) or not USER_RE.match(user):
        return error("login_reply", "invalid_username",
                     "nome deve ter 1-32 chars [A-Za-z0-9_-]")
    db["logins"].append({"user": user, "timestamp": req["timestamp"]})
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
        {"type": "channel_sync", "timestamp": now_ms(),
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
    publication = {"type": "publication", "timestamp": now_ms(), "server": NAME,
                   "channel": channel, "user": req.get("user"), "message": text}
    # persiste antes de publicar: nada que foi publicado fica sem registro
    append_message({**publication, "request_timestamp": req["timestamp"]})
    pub.send_multipart([channel.encode("utf-8"),
                        msgpack.packb(publication, use_bin_type=True)])
    log("PUBLICADO", publication)
    return reply("publish_reply", message=f"publicado em '{channel}'")


HANDLERS = {"login": do_login, "channel": do_channel,
            "channels": do_channels, "publish": do_publish}


def handle(raw):
    try:
        req = msgpack.unpackb(raw, raw=False)
        assert isinstance(req, dict) and isinstance(req.get("timestamp"), int)
    except Exception:
        return error("error", "invalid_message", "mensagem malformada ou sem timestamp")
    log("<< RECEBIDO", req)
    handler = HANDLERS.get(req.get("type"))
    if handler is None:
        return error("error", "unknown_type", f"tipo desconhecido: {req.get('type')}")
    return handler(req)


def handle_sync(frames):
    try:
        data = msgpack.unpackb(frames[1], raw=False)
        channel = data["channel"]
    except Exception:
        return
    if (data.get("origin") != NAME and isinstance(channel, str)
            and CHANNEL_RE.match(channel) and channel not in db["channels"]):
        db["channels"].append(channel)
        save(db)
        log("<< SYNC", data)


def main():
    time.sleep(1)  # dá tempo das conexões PUB/SUB se estabelecerem (slow joiner)
    log("INICIADO", {"canais": db["channels"]})
    poller = zmq.Poller()
    poller.register(rep, zmq.POLLIN)
    poller.register(sub, zmq.POLLIN)
    while True:
        events = dict(poller.poll())
        if sub in events:
            handle_sync(sub.recv_multipart())
        if rep in events:
            resp = handle(rep.recv())
            log(">> ENVIADO", resp)
            rep.send(msgpack.packb(resp, use_bin_type=True))


if __name__ == "__main__":
    main()
