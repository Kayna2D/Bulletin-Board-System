# BBS - Partes 1 a 3 (login/canais, Pub/Sub, relógios e heartbeat)
    docker compose up --build

Portas: broker REQ/REP 5555 (clientes) / 5556 (servidores); proxy Pub/Sub XSUB 5557 / XPUB 5558;
referência REP 5559 (só servidores).
Dados por servidor: ./data/<servidor>/data.json (logins, canais) e messages.jsonl (publicações).
Todas as mensagens têm `timestamp` (ms) e `clock` (relógio lógico do remetente).
