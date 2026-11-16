# BBS distribuído (CC7261) — versão final

Sistema de mensagens estilo BBS/IRC com bots, ZeroMQ, MessagePack e Docker Compose.
Servidores, serviço de referência, broker e proxy em **Python**; clientes (bots) em **Java**.

    docker compose up --build

## Componentes

| Serviço | Linguagem | Papel | Portas |
|---|---|---|---|
| `broker` | Python | proxy REQ/REP: ROUTER (clientes) ⇄ DEALER (servidores), round-robin | 5555 / 5556 |
| `proxy` | Python | proxy Pub/Sub: XSUB ⇄ XPUB | 5557 / 5558 |
| `reference` | Python | rank, endereços, lista de servidores e heartbeat | 5559 |
| `server-a/b/c` | Python | login, canais, publicações, relógios, eleição, replicação | REP entre servidores 5560 |
| `client-ana/bob` | Java | bots | — |

Mensagens são serializadas em **MessagePack** (binário) e todas têm `timestamp` (ms) e `clock` (relógio lógico de Lamport do remetente).
Tópicos Pub/Sub: nome do canal (publicações), `servers` (anúncio do coordenador) e `replica` (replicação). Os dois últimos são reservados e não podem ser usados como nome de canal.

Resumo das partes: (1) login e canais; (2) publicação Pub/Sub; (3) relógio lógico + serviço de referência com heartbeat a cada 10 mensagens de clientes; (4) eleição Bully (maior rank vence), anúncio em `servers` e sincronização de relógio estilo Berkeley a cada 15 mensagens; (5) replicação (abaixo).
`CLOCK_SKEW_MS` no compose simula relógios físicos errados para demonstrar o Berkeley (use `"0"` para desligar).

## Parte 5: replicação dos dados

### Problema
O broker distribui as requisições em round-robin, então cada servidor via só uma fração dos logins, canais e mensagens. Se um servidor caísse, parte do histórico seria perdida e um cliente receberia apenas parte dele. O requisito é que **todos os servidores tenham todos os dados**.

### Método escolhido (conceitos da aula de Consistência e replicação)
- **Modelo de consistência: consistência eventual.** Escritas são aceitas em qualquer servidor e propagadas às demais réplicas; se não houver novas escritas, todas as atualizações chegam a todas as réplicas e as cópias ficam idênticas.
- **Distribuição de conteúdo: réplica passiva.** O que circula entre as réplicas é o **dado já gravado** (o registro), e não a operação a ser reexecutada em cada uma (que seria réplica ativa).
- **Atualização por push e por pull.** *Push*: o servidor que atendeu o cliente publica o registro novo para os demais (tópico `replica`). *Pull*: periodicamente, e quando entra no sistema, cada servidor pergunta aos outros o que lhe falta. O pull cobre falhas do push.
- **Conflitos.** Os dados do projeto só crescem (login, criação de canal, mensagem são registros imutáveis), então não há atualização conflitante sobre o mesmo item. O único conflito possível é dois bots criarem o mesmo canal ao mesmo tempo em servidores diferentes; a resolução é determinística: canal é identificado pelo nome, então os dois registros resultam em um único canal.

Por que não um modelo mais forte (sequencial, linearização) ou uma réplica primária fixa: as requisições já chegam a qualquer servidor pelo broker, e centralizar a escrita (por exemplo no coordenador da Parte 4) criaria um ponto único de falha e uma janela sem escritas a cada reeleição. O custo de sincronização global desses modelos também não se justifica para dados imutáveis.

### Como foi implementado
1. **Registro replicável.** Todo dado vira um registro imutável `{id, origin, seq, kind, clock, timestamp, ...}`; `kind` é `login`, `channel` ou `message`, `origin` é o servidor que o criou e `seq` é um contador por origem (id = `origem:seq`). A lista de canais é derivada do log.
2. **Log persistente.** Cada servidor guarda `data/<servidor>/log.jsonl`, um registro por linha, com os registros de **todas** as origens. Escritas locais fazem `fsync` antes de responder ao cliente. (Substitui o `data.json` e o `messages.jsonl` das partes anteriores.)
3. **Push.** Ao criar um registro o servidor o persiste e o publica no tópico `replica` (mensagem com `timestamp` e `clock`). Quem recebe o aplica de forma **idempotente** (chave `origem:seq`; duplicados são ignorados).
4. **Pull (reconciliação).** O servidor envia a cada peer, por REQ/REP, o maior `seq` contínuo que conhece de cada origem e recebe os registros que faltam, em páginas de até `SYNC_LIMIT` (500). Ocorre: na partida do servidor; a cada heartbeat; **a cada `ANTI_ENTROPY_INTERVAL` segundos (padrão 10), mesmo sem tráfego de clientes**; e quando chega um `publish` em canal desconhecido, antes de recusá-lo.
5. **Recuperação.** Servidor que volta ou entra tarde recebe o que perdeu na partida. Se o disco local for perdido, ele recupera inclusive os próprios registros e retoma o `seq` sem repetir ids.
6. **Histórico completo.** Requisição `history` (`channel`, `limit`) devolve as últimas mensagens do canal, de todos os servidores, ordenadas por `(clock, origem, seq)`. O relógio lógico serve apenas para **ordenar** a exibição.

### Mudanças em relação ao método geral
- Sem réplica líder: unicidade e ordem vêm de `(origem, seq)` e do relógio de Lamport, e não de um sequenciador central.
- O push usa Pub/Sub (não bloqueia o atendimento do cliente e não tem confirmação) combinado com pull periódico, em vez de enviar a cada réplica e esperar confirmação.
- `fsync` só nas escritas locais; registros recebidos de outros servidores são apenas gravados (`flush`), porque a durabilidade vem das outras cópias.

### Garantias e limitações
- **Consistência eventual, sem garantia de ordem entre réplicas.** Não oferece consistência sequencial, linearização nem causal. Duas réplicas podem, por instantes, ter conjuntos diferentes de registros, e uma réplica pode receber um registro antes da causa dele; o relógio lógico não impede isso, apenas ordena o histórico. Convergência: normalmente milissegundos pelo push; no pior caso, até o próximo intervalo de reconciliação (10 s por padrão).
- **Consistência centrada no cliente não é garantida.** Como o broker alterna os servidores a cada requisição, um bot pode, por instantes, não ler o que acabou de escrever (leitura do próprio dado) nem ter leituras monotônicas (por exemplo, listar canais ou histórico num servidor e, na requisição seguinte, ver menos itens num servidor ainda não atualizado). Mitigação parcial: o `publish` em canal desconhecido faz um pull antes de falhar. Uma solução completa exigiria o cliente informar a versão que já viu e o servidor sincronizar antes de responder.
- Dois bots criando o **mesmo canal** simultaneamente em servidores diferentes podem ambos receber sucesso; o canal aparece uma vez no estado.
- Um registro que só existia num servidor que caiu antes de replicá-lo é recuperado quando ele volta; se o disco dele também for perdido, esse registro se perde.
- Os bots Java não usam `history`; ele serve para verificar a réplica por qualquer cliente.

## Como verificar a replicação

1. `docker compose up --build` e deixe os bots rodarem por alguns minutos.
2. `python tools/compare_logs.py data` compara os `log.jsonl` dos servidores (mesmos ids, buracos de `seq`); retorna `IGUAIS` ou `DIVERGENTES`.
3. `docker compose stop server-c`, espere algumas rodadas, `docker compose start server-c`: no log dele aparece `LOG SINCRONIZADO` com os registros perdidos.
4. Apague `data/server-a` e reinicie `server-a`: ele recupera tudo e volta a escrever com `seq` crescente.
5. Eleição: pare o coordenador e observe `ELEIÇÃO VENCIDA` e `COORDENADOR DEFINIDO` nos outros.
