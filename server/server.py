import json, os, re, threading, time
from datetime import datetime, timezone

import msgpack
import zmq

NAME = os.environ.get("SERVER_NAME", "server")
BROKER = os.environ.get("BROKER_BACKEND", "tcp://broker:5556")
PUB_PROXY = os.environ.get("PUB_PROXY", "tcp://proxy:5557") # XSUB
SUB_PROXY = os.environ.get("SUB_PROXY", "tcp://proxy:5558") # XPUB
REFERENCE = os.environ.get("REFERENCE", "tcp://reference:5559")
PEER_BIND = os.environ.get("PEER_BIND", "tcp://*:5560") # REP entre outros servidores
PEER_ADDRESS = os.environ.get("PEER_ADDRESS", f"tcp://{NAME}:5560") 
SKEW_MS = int(os.environ.get("CLOCK_SKEW_MS", "0")) # simula relógio físico errado
HEARTBEAT_EVERY = int(os.environ.get("HEARTBEAT_EVERY", "10")) # mensagens de clientes
CLOCK_SYNC_EVERY = int(os.environ.get("CLOCK_SYNC_EVERY", "15"))
ELECTION_WAIT = float(os.environ.get("ELECTION_WAIT", "5")) # segundos para aguardar respostas de eleição
DATA_DIR = os.environ.get("DATA_DIR", "/data")
DATA_FILE = os.path.join(DATA_DIR, "data.json") # logins + canais
MESSAGES_FILE = os.path.join(DATA_DIR, "messages.jsonl") # publicações (append-only)

USER_RE = re.compile(r"^[A-Za-z0-9_-]{1,32}$")
CHANNEL_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,31}$") # não começa com "_" (reservado)
RESERVED_CHANNELS = {"_servers"} # tópico do anúncio do coordenador
SYNC_TOPIC = b"_sync" # tópico interno: replicação de canais
SERVERS_TOPIC = b"_servers" # tópico interno: anúncio do coordenador
MAX_MSG_LEN = 1000

# relógio lógico (thread-safe: thread principal + thread de peers)
class LogicalClock:
    def __init__(self):
        self.value = 0

        self._lock = threading.Lock()

    def tick(self): # incrementa antes de cada envio
        with self._lock:    
            self.value += 1
            return self.value

    def update(self, received): # ao receber: máximo entre o recebido e o local
        with self._lock:
            self.value = max(self.value, received)
            return self.value


clock = LogicalClock()

# relógio físico (ajustado pelo coordenador) 
offset_ms = 0.0
phys_lock = threading.Lock()

def raw_now_ms():
    return time.time() * 1000 + SKEW_MS

def now_f():
    return raw_now_ms() + offset_ms

def now_ms():
    return int(now_f())

def adjust_to_coordinator(coord_time, t0, t1):
    """Hora do coordenador (já média de Berkeley) + RTT/2 de compensação"""
    global offset_ms
    rtt = t1 - t0
    with phys_lock:
        offset_ms = (coord_time + rtt / 2) - t1
    log("RELÓGIO AJUSTADO", {"offset_ms": round(offset_ms, 2), "rtt_ms": round(rtt, 2), "coordenador": coordinator})

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
coordinator = None # nome do servidor eleito coordenador
peers = {} # nome -> {"name", "rank", "address"} (da referência)
election_requested = threading.Event()
election_deadline = 0.0

# sockets da thread principal
ctx = zmq.Context()
rep = ctx.socket(zmq.REP)
rep.connect(BROKER)
pub = ctx.socket(zmq.PUB)
pub.connect(PUB_PROXY)
sub = ctx.socket(zmq.SUB) # replicação de canais + anúncios do coordenador
sub.connect(SUB_PROXY)
sub.setsockopt(zmq.SUBSCRIBE, SYNC_TOPIC)
sub.setsockopt(zmq.SUBSCRIBE, SERVERS_TOPIC)
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
        ref.send(msgpack.packb(req, use_bin_type=True))
        raw = ref.recv()
    except zmq.ZMQError:
        log("!! REF sem resposta", {"type": rtype})
        connect_reference() # REQ travado após timeout: recria o socket
        return None
    resp = msgpack.unpackb(raw, raw=False)
    clock.update(resp.get("clock", 0))
    log("<< REF RECEBIDO", resp)
    return resp


def register():
    """Pede o rank (informando o endereço); repete até a referência responder."""
    global rank
    while True:
        resp = ask_reference("rank", address=PEER_ADDRESS)
        if resp and resp.get("status") == "ok":
            rank = resp["rank"]
            log("REGISTRADO", {"name": NAME, "rank": rank, "address": PEER_ADDRESS})
            return
        time.sleep(2)

def refresh_peers():
    global peers
    lst = ask_reference("list")
    if lst and lst.get("status") == "ok":
        peers = {s["name"]: s for s in lst["servers"]}
        log("SERVIDORES DISPONÍVEIS", lst["servers"])


def sync_channels():
    """Servidor tardio (ou que perdeu um _sync): puxa os canais dos outros servidores e faz o merge."""
    added = []
    for name, p in list(peers.items()):
        if name == NAME or not p.get("address"):
            continue
        resp = peer_request(p["address"], "channels_sync", 500)
        if not peer_ok(resp) or not isinstance(resp.get("channels"), list):
            continue
        for ch in resp["channels"]:
            if valid_channel(ch) and ch not in db["channels"]:
                db["channels"].append(ch)
                added.append(ch)
    if added:
        save(db)
        log("CANAIS SINCRONIZADOS", {"novos": added, "total": len(db["channels"])})

def heartbeat():
    global rank
    resp = ask_reference("heartbeat", address=PEER_ADDRESS)
    if resp and resp.get("status") == "ok" and resp.get("rank") != rank:
        rank = resp["rank"]
        log("RANK ATUALIZADO", {"rank": rank})
    refresh_peers()
    sync_channels() # garante que nenhum canal ficou para trás

# comunicação entre servidores (REQ/REP) 
def peer_request(address, rtype, timeout_ms=1000, **extra):
    """REQ a outro servidor com um socket novo (seguro entre threads). None se não responder."""
    req = {"type": rtype, "timestamp": now_ms(), "clock": clock.tick(),
           "name": NAME, "rank": rank, **extra}
    log(">> PEER ENVIADO", {"para": address, **req})
    s = ctx.socket(zmq.REQ)
    try:
        s.setsockopt(zmq.LINGER, 0)
        s.setsockopt(zmq.RCVTIMEO, timeout_ms)
        s.setsockopt(zmq.SNDTIMEO, timeout_ms)
        s.connect(address)
        s.send(msgpack.packb(req, use_bin_type=True))
        raw = s.recv()
    except zmq.ZMQError:
        log("!! PEER sem resposta", {"para": address, "type": rtype})
        return None
    finally:
        s.close(linger=0)
    resp = msgpack.unpackb(raw, raw=False)
    clock.update(resp.get("clock", 0))
    log("<< PEER RECEBIDO", resp)
    return resp


def peer_ok(resp):
    return bool(resp) and resp.get("status") == "ok"


def berkeley_round():
    """Coordenador: consulta o relógio de todos, calcula a média e ajusta o próprio relógio."""
    global offset_ms
    diffs = {NAME: 0.0}
    for name, p in list(peers.items()):
        if name == NAME or not p.get("address"):
            continue
        t0 = now_f()
        resp = peer_request(p["address"], "time_poll", 800)
        t1 = now_f()
        if peer_ok(resp) and isinstance(resp.get("time"), int):
            diffs[name] = (resp["time"] + (t1 - t0) / 2) - t1 # diferença p/ meu relógio
    avg = sum(diffs.values()) / len(diffs)
    with phys_lock:
        offset_ms += avg
    log("BERKELEY", {"diferencas_ms": {k: round(v, 1) for k, v in diffs.items()},
                     "ajuste_medio_ms": round(avg, 1)})


def handle_peer(raw):
    try:
        req = msgpack.unpackb(raw, raw=False)
        assert isinstance(req, dict)
        assert isinstance(req.get("timestamp"), int) and isinstance(req.get("clock"), int)
    except Exception:
        return error("error", "invalid_message", "mensagem malformada (timestamp/clock obrigatórios)")
    clock.update(req["clock"])
    log("<< PEER RECEBIDO", req)
    rtype = req.get("type")
    if rank is None:
        return error(f"{rtype}_reply", "not_ready", "servidor ainda sem rank")
    if rtype == "election":
        sender_rank = req.get("rank")
        if isinstance(sender_rank, int) and sender_rank < rank:
            election_requested.set() # Bully: o de rank maior assume a eleição
        return reply("election_reply", message="OK")
    if rtype == "channels_sync": # outro servidor pedindo meus canais
        return reply("channels_sync_reply", channels=list(db["channels"]))
    if rtype == "time_poll": # coordenador perguntando a minha hora
        return reply("time_poll_reply", time=now_ms())
    if rtype == "clock": # servidor pedindo a hora correta
        if coordinator != NAME:
            return error("clock_reply", "not_coordinator", f"coordenador é {coordinator}")
        berkeley_round()
        return reply("clock_reply", time=now_ms())
    return error("error", "unknown_type", f"tipo desconhecido: {rtype}")


def peer_loop():
    sock = ctx.socket(zmq.REP)
    sock.bind(PEER_BIND)
    while True:
        resp = handle_peer(sock.recv())
        log(">> PEER ENVIADO", resp)
        sock.send(msgpack.packb(resp, use_bin_type=True))


# eleição (Bully: vence o maior rank) 
def announce_coordinator():
    msg = {"type": "coordinator", "timestamp": now_ms(), "clock": clock.tick(),
           "coordinator": NAME, "rank": rank}
    pub.send_multipart([SERVERS_TOPIC, msgpack.packb(msg, use_bin_type=True)])
    log(">> ANUNCIO [servers]", msg)


def run_election():
    global coordinator, election_deadline
    refresh_peers()
    log("ELEIÇÃO INICIADA", {"rank": rank})
    got_ok = False
    for p in sorted(peers.values(), key=lambda x: x["rank"]):
        if p["rank"] > rank and p.get("address"):
            if peer_ok(peer_request(p["address"], "election")):
                got_ok = True
    if got_ok:
        coordinator = None # espera o anúncio do vencedor
        election_deadline = time.monotonic() + ELECTION_WAIT
        log("ELEIÇÃO: servidor de rank maior respondeu OK, aguardando anúncio", {})
    else:
        coordinator = NAME
        log("ELEIÇÃO VENCIDA: sou o coordenador", {"coordenador": coordinator})
        announce_coordinator()


def sync_clock():
    """A cada 15 mensagens: pede a hora ao coordenador. Se ele não responde, nova eleição."""
    if coordinator is None or coordinator == NAME:
        return
    p = peers.get(coordinator)
    if not p or not p.get("address"):
        log("!! coordenador fora da lista de servidores", {"coordenador": coordinator})
        run_election()
        return
    t0 = raw_now_ms()
    resp = peer_request(p["address"], "clock", 3000)
    t1 = raw_now_ms()
    if peer_ok(resp) and isinstance(resp.get("time"), int):
        adjust_to_coordinator(resp["time"], t0, t1)
    else:
        log("!! coordenador indisponível", {"coordenador": coordinator})
        run_election()

# respostas 
def reply(rtype, status="ok", **extra):
    return {"type": rtype, "timestamp": now_ms(),
            "clock": clock.tick(),
            "status": status, "server": NAME, **extra}


def error(rtype, code, message):
    return reply(rtype, "error", error=code, message=message)


# handlers de clientes
def valid_channel(name):
    return isinstance(name, str) and CHANNEL_RE.match(name) and name not in RESERVED_CHANNELS

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
    if not valid_channel(name):
        return error("channel_reply", "invalid_channel",
                     "canal deve ter 1-32 chars [A-Za-z0-9_-], não começar com '_' e nem ser reservado")
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
    if not valid_channel(channel):
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
        assert isinstance(req.get("timestamp"), int) and isinstance(req.get("clock"), int)
    except Exception:
        return error("error", "invalid_message", "mensagem malformada (timestamp/clock obrigatórios)")
    clock.update(req["clock"])
    log("<< RECEBIDO", req)
    handler = HANDLERS.get(req.get("type"))
    if handler is None:
        return error("error", "unknown_type", f"tipo desconhecido: {req.get('type')}")
    return handler(req)

# mensagens Pub/Sub entre servidores
def handle_channel_sync(data):
    channel = data.get("channel")
    if (data.get("origin") != NAME and valid_channel(channel) 
        and channel not in db["channels"]):
        db["channels"].append(channel)
        save(db)
        log("<< SYNC", data)

def handle_coordinator(data):
    global coordinator
    name, sender_rank = data.get("coordinator"), data.get("rank")
    if not isinstance(name, str) or name == NAME:
        return
    log("<< ANUNCIO [servers]", data)
    if isinstance(sender_rank, int) and rank is not None and sender_rank < rank:
        log("anúncio de rank menor que o meu: contestando", {})
        election_requested.set() # eu venceria: faço nova eleição
        return
    coordinator = name
    log("COORDENADOR DEFINIDO", {"coordenador": coordinator})


def handle_sub(frames):
    try:
        topic, data = frames[0], msgpack.unpackb(frames[1], raw=False)
        clock.update(data["clock"])
    except Exception:
        return
    if topic == SYNC_TOPIC:
        handle_channel_sync(data)
    elif topic == SERVERS_TOPIC and data.get("type") == "coordinator":
        handle_coordinator(data)


def main():
    threading.Thread(target=peer_loop, daemon=True, name="peers").start()
    time.sleep(1) # dá tempo das conexões PUB/SUB se estabelecerem (slow joiner)
    connect_reference()
    register()
    refresh_peers()
    sync_channels() # entra conhecendo os canais existentes
    log("INICIADO", {"rank": rank, "canais": db["channels"]})
    run_election()
    poller = zmq.Poller()
    poller.register(rep, zmq.POLLIN)
    poller.register(sub, zmq.POLLIN)
    handled = 0
    while True:
        events = dict(poller.poll(500))
        if sub in events:
            handle_sub(sub.recv_multipart())
        if rep in events:
            resp = handle(rep.recv())
            log(">> ENVIADO", resp)
            rep.send(msgpack.packb(resp, use_bin_type=True))
            handled += 1
            if handled % HEARTBEAT_EVERY == 0: # heartbeat + lista de servidores
                heartbeat()
            if handled % CLOCK_SYNC_EVERY == 0: # relógio com o coordenador
                sync_clock()
        if election_requested.is_set():
            election_requested.clear()
            run_election()
        elif coordinator is None and time.monotonic() > election_deadline:
            run_election() # ninguém anunciou: tenta de novo



if __name__ == "__main__":
    main()