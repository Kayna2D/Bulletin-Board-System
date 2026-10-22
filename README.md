# BBS - Partes 1 e 2 (login, canais, publicação Pub/Sub)
    docker compose up --build

Portas: broker REQ/REP 5555 (clientes) / 5556 (servidores); proxy Pub/Sub XSUB 5557 / XPUB 5558.
Dados por servidor: ./data/<servidor>/data.json (logins, canais) e messages.jsonl (publicações).
