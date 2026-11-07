# BBS - Partes 1 a 4 (login/canais, Pub/Sub, relógios/heartbeat, eleição)
    docker compose up --build

Portas: broker REQ/REP 5555 (clientes) / 5556 (servidores); proxy Pub/Sub XSUB 5557 / XPUB 5558;
referência REP 5559 (só servidores); cada servidor tem um REP próprio na 5560 (eleição e relógio).
Tópicos Pub/Sub: nome do canal (publicações), `servers` (anúncio do coordenador), `_sync` (replicação de canais).
Dados por servidor: ./data/<servidor>/data.json (logins, canais) e messages.jsonl (publicações).
Todas as mensagens têm `timestamp` (ms) e `clock` (relógio lógico do remetente).

Testar a eleição: docker compose stop server-c   (coordenador cai -> novo coordenador em ~15 mensagens)
