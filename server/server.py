import json, os, re, time
from datetime import datetime, timezone

import msgpack
import zmq

NAME = os.environ.get("SERVER_NAME", "server")
BROKER = os.environ.get("BROKER_BACKEND", "tcp://broker:5556")
DATA_FILE = os.path.join(os.environ.get("DATA_DIR", "/data"), "data.json")
NAME_RE = re.compile(r"^[A-Za-z0-9_-]{1,32}$")


def log(tag, msg):
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
    print(f"[{ts}] [{NAME}] {tag} {json.dumps(msg, ensure_ascii=False)}", flush=True)


# persistência (cada servidor tem o seu arquivo) 
def load():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, encoding="utf-8") as f:
            return json.load(f)
    return {"logins": [], "channels": []}


def save(db):
    os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)
    tmp = DATA_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(db, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, DATA_FILE)  # escrita atômica


db = load()


# respostas 
def reply(rtype, status="ok", **extra):
    return {"type": rtype, "timestamp": int(time.time() * 1000),
            "status": status, "server": NAME, **extra}


def error(rtype, code, message):
    return reply(rtype, "error", error=code, message=message)


# handlers 
def do_login(req):
    user = req.get("user")
    if not isinstance(user, str) or not NAME_RE.match(user):
        return error("login_reply", "invalid_username",
                     "nome deve ter 1-32 chars [A-Za-z0-9_-]")
    db["logins"].append({"user": user, "timestamp": req["timestamp"]})
    save(db)
    return reply("login_reply", message=f"bem-vindo(a) {user}")


def do_channel(req):
    name = req.get("channel")
    if not isinstance(name, str) or not NAME_RE.match(name):
        return error("channel_reply", "invalid_channel",
                     "canal deve ter 1-32 chars [A-Za-z0-9_-]")
    if name in db["channels"]:
        return error("channel_reply", "channel_exists", f"canal '{name}' já existe")
    db["channels"].append(name)
    save(db)
    return reply("channel_reply", message=f"canal '{name}' criado")


def do_channels(req):
    return reply("channels_reply", channels=list(db["channels"]))


HANDLERS = {"login": do_login, "channel": do_channel, "channels": do_channels}


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


def main():
    ctx = zmq.Context()
    sock = ctx.socket(zmq.REP)
    sock.connect(BROKER)
    log("INICIADO", {"broker": BROKER, "canais": db["channels"]})
    while True:
        resp = handle(sock.recv())
        log(">> ENVIADO", resp)
        sock.send(msgpack.packb(resp, use_bin_type=True))


if __name__ == "__main__":
    main()
