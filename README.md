# BBS - Partes 1 a 4 (login/canais, Pub/Sub, relógios, heartbeat e eleição)
    docker compose up --build

Portas: broker REQ/REP 5555 (clientes) / 5556 (servidores); proxy Pub/Sub XSUB 5557 / XPUB 5558;
referência REP 5559 (só servidores).
Dados por servidor: ./data/<servidor>/data.json (logins, canais) e messages.jsonl (publicações).
Todas as mensagens têm `timestamp` (ms) e `clock` (relógio lógico do remetente).
Servidores elegem um coordenador baseado no maior rank e publicam decisão no tópico `servers`
A cada 15 mensagens de cliente, o servidor (não coordenador) envia `REQ: relógio` ao coordenador; o coordenador coleta as diferenças, calcula o ajuste médio e responde `REP: hora correta`. O servidor aplica o offset e registra `RELÓGIO AJUSTADO`.