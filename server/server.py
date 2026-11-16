import json, os, re, threading, time
from datetime import datetime, timezone

import msgpack
import zmq

NAME = os.environ.get("SERVER_NAME", "server")
BROKER = os.environ.get("BROKER_BACKEND", "tcp://broker:5556")
PUB_PROXY = os.environ.get("PUB_PROXY", "tcp://proxy:5557") # XSUB
SUB_PROXY = os.environ.get("SUB_PROXY", "tcp://proxy:5558") # XPUB
REFERENCE = os.environ.get("REFERENCE", "tcp://reference:5559")
PEER_BIND = os.environ.get("PEER_BIND", "tcp://*:5560") # REP entre servidores
PEER_ADDRESS = os.environ.get("PEER_ADDRESS", f"tcp://{NAME}:5560") 
SKEW_MS = int(os.environ.get("CLOCK_SKEW_MS", "0")) # simula relógio físico errado
HEARTBEAT_EVERY = int(os.environ.get("HEARTBEAT_EVERY", "10")) # mensagens de clientes
CLOCK_SYNC_EVERY = int(os.environ.get("CLOCK_SYNC_EVERY", "15"))
ELECTION_WAIT = float(os.environ.get("ELECTION_WAIT", "5")) # segundos para aguardar respostas de eleição
SYNC_LIMIT = int(os.environ.get("SYNC_LIMIT", "500")) # registros por resposta de anti-entropia
AE_INTERVAL = float(os.environ.get("ANTI_ENTROPY_INTERVAL", "10")) # segundos entre reconciliações (0 = desliga)
SUSPECT_SECONDS = float(os.environ.get("SUSPECT_SECONDS", "20")) # s sem tentar um peer que não respondeu
DATA_DIR = os.environ.get("DATA_DIR", "/data")
LOG_FILE = os.path.join(DATA_DIR, "log.jsonl") # log replicado (logins, canais, mensagens)
USER_RE = re.compile(r"^[A-Za-z0-9_-]{1,32}$")
CHANNEL_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,31}$") # não começa com "_" (reservado)
SERVERS_TOPIC = b"servers" # anúncio do coordenador
REPLICA_TOPIC = b"replica" # replicação de registros
RESERVED_CHANNELS = {"servers", "replica"} # nomes de tópicos internos
KINDS = {"login", "channel", "message"}
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

def adjust_to_reference(reference_time, t0, t1):
    """Ajusta o relógio físico usando a hora da referência e metade do RTT."""
    global offset_ms
    rtt = t1 - t0
    with phys_lock:
        offset_ms = (reference_time + rtt / 2) - t1
    log("RELÓGIO AJUSTADO", {"offset_ms": round(offset_ms, 2), "rtt_ms": round(rtt, 2), "referência": REFERENCE})

def log(tag, msg):
    ts = datetime.fromtimestamp(now_ms() / 1000, timezone.utc).strftime("%H:%M:%S.%f")[:-3]
    print(f"[{ts}] [{NAME}] {tag} {json.dumps(brief(msg), ensure_ascii=False)}", flush=True)

def brief(msg):
    """Resume listas grandes de registros nos logs (anti-entropia)."""
    if isinstance(msg, dict) and isinstance(msg.get("records"), list):
        msg = {**msg, "records": f"<{len(msg['records'])} registros>"}
    if isinstance(msg, dict) and isinstance(msg.get("have"), dict):
        msg = {**msg, "have": dict(msg["have"])}
    return msg


# Réplica dos dados
# Todo dado (login, criação de canal, mensagem publicada) vira um REGISTRO
# imutável com id (origem, seq). O log de cada servidor é um conjunto de
# registros de todas as origens. Escritas são aceitas em qualquer servidor
# (update-anywhere), propagadas por push (Pub/Sub, tópico `replica`) e
# reconciliadas por anti-entropia (pull via REQ/REP entre servidores).
store_lock = threading.RLock()
records = {} # origem -> {seq: registro}
contig = {} # origem -> maior seq sem buracos (vetor de versões)
channels = [] # estado derivado do log
local_seq = 0 # último seq criado por este servidor

def valid_int(x):
    return isinstance(x, int) and not isinstance(x, bool)

def valid_channel(name):
    return isinstance(name, str) and CHANNEL_RE.match(name) and name not in RESERVED_CHANNELS

def valid_record(rec):
    try:
        if not isinstance(rec, dict) or rec.get("kind") not in KINDS:
            return False
        if not (isinstance(rec.get("origin"), str) and USER_RE.match(rec["origin"])):
            return False
        if not (valid_int(rec.get("seq")) and rec["seq"] > 0):
            return False
        if not (valid_int(rec.get("clock")) and valid_int(rec.get("timestamp"))):
            return False
        if rec["kind"] == "login":
            return isinstance(rec.get("user"), str)
        if rec["kind"] == "channel":
            return valid_channel(rec.get("channel"))
        return valid_channel(rec.get("channel")) and isinstance(rec.get("message"), str)
    except Exception:
        return False


def store_record(rec, mode):
    """Guarda o registro se for novo (idempotente). mode: none | flush | fsync."""
    global local_seq
    origin, seq = rec["origin"], rec["seq"]
    with store_lock:
        by_seq = records.setdefault(origin, {})
        if seq in by_seq:
            return False
        by_seq[seq] = rec
        c = contig.get(origin, 0)
        while c + 1 in by_seq:
            c += 1
        contig[origin] = c
        if origin == NAME and seq > local_seq: # recupera o contador se o disco local foi perdido
            local_seq = seq
        if rec["kind"] == "channel" and rec["channel"] not in channels:
            channels.append(rec["channel"])
        if mode != "none":
            os.makedirs(DATA_DIR, exist_ok=True)
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                f.flush()
                if mode == "fsync": # só escritas locais esperam o disco
                    os.fsync(f.fileno())
        return True    

def load_log():
    if not os.path.exists(LOG_FILE):
        return
    bad = 0
    with open(LOG_FILE, encoding="utf-8") as f:
        for line in f:
            try:
                rec = json.loads(line)
            except ValueError:
                bad += 1 # última linha pode estar incompleta após queda
                continue
            if valid_record(rec):
                store_record(rec, "none")
    log("LOG CARREGADO", {"registros": count_records(), "canais": len(channels), "linhas_invalidas": bad})

def count_records():
    with store_lock:
        return sum(len(v) for v in records.values())

def new_record(kind, **fields):
    """Cria um registro local: persiste (fsync) e propaga aos outros servidores."""
    global local_seq
    with store_lock:
        local_seq += 1
        rec = {"id": f"{NAME}:{local_seq}", "origin": NAME, "seq": local_seq, "kind": kind,
               "clock": clock.tick(), "timestamp": now_ms(), **fields}
        store_record(rec, "fsync")
    msg = {"type": "replica", "timestamp": now_ms(), "clock": clock.tick(), "record": rec}
    pub.send_multipart([REPLICA_TOPIC, msgpack.packb(msg, use_bin_type=True)])
    log(">> REPLICA ENVIADA", msg)
    return rec

def records_after(have, limit):
    """Registros que o solicitante ainda não tem (seq acima do que ele informou por origem)."""
    out = []
    with store_lock:
        for origin, by_seq in records.items():
            floor = have.get(origin, 0)
            for seq in sorted(s for s in by_seq if s > floor):
                if len(out) >= limit:
                    return out, True
                out.append(by_seq[seq])
    return out, False

def handle_replica(data):
    rec = data.get("record")
    if valid_record(rec) and rec["origin"] != NAME:
        mark_alive(rec["origin"])
        if store_record(rec, "flush"):
            log("<< REPLICA RECEBIDA", data)

rank = None
coordinator = None # nome do servidor eleito coordenador
peers = {} # nome -> {"name", "rank", "address"} (da referência)
election_requested = threading.Event()
election_deadline = 0.0

ctx = zmq.Context() # núcleo: REP dos clientes (broker) e REP entre servidores
ctx_ps = zmq.Context() # Pub/Sub (proxy)
ctx_ref = zmq.Context()# serviço de referência
_peer_ctxs = {} # endereço do peer -> contexto dedicado aos pedidos a ele
_peer_ctx_lock = threading.Lock()
suspects = {} # endereço do peer -> instante (monotonic) até o qual não o consultamos


def peer_context(address):
    with _peer_ctx_lock:
        if address not in _peer_ctxs:
            _peer_ctxs[address] = zmq.Context()
        return _peer_ctxs[address]


def slow_reconnect(sock):
    """Tentativas de reconexão espaçadas (1 s, até 5 s) para não martelar o DNS."""
    sock.setsockopt(zmq.RECONNECT_IVL, 1000)
    sock.setsockopt(zmq.RECONNECT_IVL_MAX, 5000)


def mark_alive(name):
    """Recebe algo deste servidor: deixa de ser suspeito."""
    p = peers.get(name)
    if p and p.get("address"):
        suspects.pop(p["address"], None)

rep = ctx.socket(zmq.REP)
rep.connect(BROKER)
pub = ctx_ps.socket(zmq.PUB)
slow_reconnect(pub)
pub.connect(PUB_PROXY)
sub = ctx_ps.socket(zmq.SUB) # replicação de canais + anúncios do coordenador
slow_reconnect(sub)
sub.connect(SUB_PROXY)
sub.setsockopt(zmq.SUBSCRIBE, SERVERS_TOPIC)
sub.setsockopt(zmq.SUBSCRIBE, REPLICA_TOPIC)
ref = None # REQ para o serviço de referência

# serviço de referência 
def connect_reference():
    global ref
    if ref is not None:
        ref.close(linger=0)
    ref = ctx_ref.socket(zmq.REQ)
    slow_reconnect(ref)
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

def heartbeat():
    global rank
    t0 = raw_now_ms()
    resp = ask_reference("heartbeat", address=PEER_ADDRESS)
    t1 = raw_now_ms()
    if resp and resp.get("status") == "ok" and isinstance(resp.get("time"), int):
        adjust_to_reference(resp["time"], t0, t1)
    if resp and resp.get("status") == "ok" and resp.get("rank") != rank:
        rank = resp["rank"]
        log("RANK ATUALIZADO", {"rank": rank})
    refresh_peers()
    sync_with_peers() # anti-entropia: repara qualquer registro perdido no push

# comunicação entre servidores (REQ/REP) 
def peer_request(address, rtype, timeout_ms=1000, **extra):
    """REQ a outro servidor com um socket novo (seguro entre threads). None se não responder.
    Um peer que não respondeu fica suspeito por SUSPECT_SECONDS: nesse período não é consultado
    (evita gastar timeouts e novas resoluções de DNS a cada reconciliação/eleição/Berkeley)."""
    until = suspects.get(address)
    if until is not None and time.monotonic() < until:
        return None
    req = {"type": rtype, "timestamp": now_ms(), "clock": clock.tick(),
           "name": NAME, "rank": rank, **extra}
    log(">> PEER ENVIADO", {"para": address, **req})
    s = peer_context(address).socket(zmq.REQ)
    try:
        s.setsockopt(zmq.LINGER, 0)
        s.setsockopt(zmq.RCVTIMEO, timeout_ms)
        s.setsockopt(zmq.SNDTIMEO, timeout_ms)
        s.connect(address)
        s.send(msgpack.packb(req, use_bin_type=True))
        raw = s.recv()
    except zmq.ZMQError:
        suspects[address] = time.monotonic() + SUSPECT_SECONDS
        log("!! PEER sem resposta", {"para": address, "type": rtype,
                                     "suspeito_por_s": SUSPECT_SECONDS})
        return None
    finally:
        s.close(linger=0)
    suspects.pop(address, None)
    resp = msgpack.unpackb(raw, raw=False)
    clock.update(resp.get("clock", 0))
    log("<< PEER RECEBIDO", resp)
    return resp


def peer_ok(resp):
    return bool(resp) and resp.get("status") == "ok"

def sync_with_peers():
    """Anti-entropia: informa a cada peer o que já tenho e aplica os registros que faltam."""
    added = 0
    for name, p in list(peers.items()):
        if name == NAME or not p.get("address"):
            continue
        for _ in range(50): # paginação (SYNC_LIMIT por resposta)
            with store_lock:
                have = dict(contig)
            resp = peer_request(p["address"], "log_sync", 800, have=have)
            if not peer_ok(resp) or not isinstance(resp.get("records"), list):
                break
            for rec in resp["records"]:
                if valid_record(rec) and store_record(rec, "flush"):
                    added += 1
            if not resp.get("truncated"):
                break
    if added:
        log("LOG SINCRONIZADO", {"novos_registros": added, "total": count_records(),
                                 "canais": len(channels)})
    return added


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
    mark_alive(req.get("name"))
    rtype = req.get("type")
    if rank is None:
        return error(f"{rtype}_reply", "not_ready", "servidor ainda sem rank")
    if rtype == "election":
        sender_rank = req.get("rank")
        if isinstance(sender_rank, int) and sender_rank < rank:
            election_requested.set() # Bully: o de rank maior assume a eleição
        return reply("election_reply", message="OK")
    if rtype == "log_sync": # outro servidor pedindo o que lhe falta 
        have = req.get("have")
        if not isinstance(have, dict):
            return error("log_sync_reply", "invalid_message", "campo 'have' ausente")
        have = {k: v for k, v in have.items() if isinstance(k, str) and valid_int(v)}
        recs, truncated = records_after(have, SYNC_LIMIT)
        return reply("log_sync_reply", records=recs, truncated=truncated)
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
def do_login(req):
    user = req.get("user")
    if not isinstance(user, str) or not USER_RE.match(user):
        return error("login_reply", "invalid_username",
                     "nome deve ter 1-32 chars [A-Za-z0-9_-]")
    new_record("login", user=user, request_timestamp=req["timestamp"], request_clock=req["clock"])
    return reply("login_reply", message=f"bem-vindo(a) {user}")


def do_channel(req):
    name = req.get("channel")
    if not valid_channel(name):
        return error("channel_reply", "invalid_channel",
                     "canal deve ter 1-32 chars [A-Za-z0-9_-], não começar com '_' e nem ser reservado")
    if name in channels:
        return error("channel_reply", "channel_exists", f"canal '{name}' já existe")
    new_record("channel", channel=name, user=req.get("user"))
    return reply("channel_reply", message=f"canal '{name}' criado")


def do_channels(req):
    with store_lock:
        return reply("channels_reply", channels=list(channels))


def do_publish(req):
    channel, text = req.get("channel"), req.get("message")
    if not valid_channel(channel):
        return error("publish_reply", "invalid_channel", "nome de canal inválido")
    if channel not in channels:
        sync_with_peers() # o registro do canal ainda pode estar a caminho
    if channel not in channels:
        return error("publish_reply", "channel_not_found", f"canal '{channel}' não existe")
    if not isinstance(text, str) or not text.strip() or len(text) > MAX_MSG_LEN:
        return error("publish_reply", "invalid_message",
                     f"mensagem deve ter 1-{MAX_MSG_LEN} caracteres")
    # persiste e replica antes de publicar: nada que foi publicado fica sem registro
    rec = new_record("message", channel=channel, user=req.get("user"), message=text,
                     request_timestamp=req["timestamp"], request_clock=req["clock"])
    publication = {"type": "publication", "timestamp": now_ms(),
                   "clock": clock.tick(), "server": NAME, 
                   "channel": channel, "user": req.get("user"), 
                   "message": text, "id": rec["id"]}
    pub.send_multipart([channel.encode("utf-8"),
                        msgpack.packb(publication, use_bin_type=True)])
    log("PUBLICADO", publication)
    return reply("publish_reply", message=f"publicado em '{channel}'")

def do_history(req):
    """Histórico completo de um canal (de TODOS os servidores), em ordem de relógio lógico."""
    channel, limit = req.get("channel"), req.get("limit", 50)
    if not valid_channel(channel):
        return error("history_reply", "invalid_channel", "nome de canal inválido")
    if not valid_int(limit) or not 1 <= limit <= 500:
        return error("history_reply", "invalid_limit", "limit deve estar entre 1 e 500")
    with store_lock:
        msgs = [r for by in records.values() for r in by.values()
                if r["kind"] == "message" and r["channel"] == channel]
    msgs.sort(key=lambda r: (r["clock"], r["origin"], r["seq"]))
    items = [{"id": r["id"], "timestamp": r["timestamp"], "clock": r["clock"],
              "user": r.get("user"), "message": r["message"]} for r in msgs[-limit:]]
    return reply("history_reply", channel=channel, total=len(msgs), messages=items)

HANDLERS = {"login": do_login, "channel": do_channel,
            "channels": do_channels, "publish": do_publish,
            "history": do_history}


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
def handle_coordinator(data):
    global coordinator
    name, sender_rank = data.get("coordinator"), data.get("rank")
    if not isinstance(name, str) or name == NAME:
        return
    log("<< ANUNCIO [servers]", data)
    mark_alive(name)
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
    if topic == REPLICA_TOPIC and data.get("type") == "replica":
        handle_replica(data)
    elif topic == SERVERS_TOPIC and data.get("type") == "coordinator":
        handle_coordinator(data)


def main():
    load_log()
    threading.Thread(target=peer_loop, daemon=True, name="peers").start()
    time.sleep(1) # dá tempo das conexões PUB/SUB se estabelecerem (slow joiner)
    connect_reference()
    register()
    refresh_peers()
    sync_with_peers() # entra já com todos os registros dos outros servidores
    log("INICIADO", {"rank": rank, "registros": count_records(), "canais": channels})
    run_election()
    poller = zmq.Poller()
    poller.register(rep, zmq.POLLIN)
    poller.register(sub, zmq.POLLIN)
    handled = 0
    next_ae = time.monotonic() + AE_INTERVAL 
    while True:
        events = dict(poller.poll(500))
        if sub in events:
            handle_sub(sub.recv_multipart())
        if rep in events:
            resp = handle(rep.recv())
            log(">> ENVIADO", resp)
            rep.send(msgpack.packb(resp, use_bin_type=True))
            handled += 1
            if handled % HEARTBEAT_EVERY == 0: # heartbeat + lista + anti-entropia
                heartbeat()
            if handled % CLOCK_SYNC_EVERY == 0: # relógio com o coordenador
                sync_clock()
        if election_requested.is_set():
            election_requested.clear()
            run_election()
        elif coordinator is None and time.monotonic() > election_deadline:
            run_election() # ninguém anunciou: tenta de novo
        if AE_INTERVAL > 0 and time.monotonic() >= next_ae:
            sync_with_peers() # reconcília por tempo, mesmo sem tráfego
            next_ae = time.monotonic() + AE_INTERVAL


if __name__ == "__main__":
    main()