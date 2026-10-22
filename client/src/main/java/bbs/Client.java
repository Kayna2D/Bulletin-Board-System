package bbs;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Client {
    static final ObjectMapper MAPPER = new ObjectMapper(new MessagePackFactory());
    static final String BROKER = env("BROKER_FRONTEND", "tcp://broker:5555");
    static final String SUB_PROXY = env("SUB_PROXY", "tcp://proxy:5558");
    static final String USER = env("BOT_NAME", "bot");
    static final int TIMEOUT_MS = 3000;
    static final int MIN_CHANNELS = 5, MIN_SUBSCRIPTIONS = 3, MSGS_PER_ROUND = 10;
    static final Random RND = new Random();

    static final ZContext ctx = new ZContext();       // REQ (thread principal)
    static ZMQ.Socket sock;

    // estado compartilhado com a thread do SUB
    static final Set<String> SUBSCRIBED = ConcurrentHashMap.newKeySet();
    static final Queue<String> PENDING = new ConcurrentLinkedQueue<>();

    static String env(String k, String d) {
        String v = System.getenv(k);
        return (v == null || v.isBlank()) ? d : v;
    }

    static void log(String tag, Object m) {
        System.out.printf("[%s] [%s] %s %s%n", Instant.now(), USER, tag, m);
    }

    // REQ/REP 
    static void connect() {
        if (sock != null) ctx.destroySocket(sock);
        sock = ctx.createSocket(SocketType.REQ);
        sock.setReceiveTimeOut(TIMEOUT_MS);
        sock.setLinger(0);
        sock.connect(BROKER);
    }

    /** Envia uma requisição; retorna null em timeout (recria o socket REQ). */
    static Map<String, Object> request(String type, Map<String, Object> extra) throws Exception {
        Map<String, Object> req = new LinkedHashMap<>();
        req.put("type", type);
        req.put("timestamp", System.currentTimeMillis());
        req.put("user", USER);
        req.putAll(extra);
        log(">> ENVIADO", req);
        sock.send(MAPPER.writeValueAsBytes(req), 0);
        byte[] raw = sock.recv(0);
        if (raw == null) {
            log("!!", "timeout, reconectando");
            connect();
            return null;
        }
        Map<String, Object> rep = MAPPER.readValue(raw, new TypeReference<>() {});
        log("<< RECEBIDO", rep);
        return rep;
    }

    static boolean ok(Map<String, Object> rep) {
        return rep != null && "ok".equals(rep.get("status"));
    }

    /** Retorna a lista de canais, ou null se a requisição falhou. */
    @SuppressWarnings("unchecked")
    static List<String> listChannels() throws Exception {
        Map<String, Object> rep = request("channels", Map.of());
        return ok(rep) ? new ArrayList<>((List<String>) rep.get("channels")) : null;
    }

    // SUB (thread própria) 
    static void startSubscriber() {
        Thread t = new Thread(() -> {
            try (ZContext subCtx = new ZContext()) {
                ZMQ.Socket sub = subCtx.createSocket(SocketType.SUB);
                sub.connect(SUB_PROXY);
                sub.setReceiveTimeOut(200);
                while (!Thread.currentThread().isInterrupted()) {
                    String topic;
                    while ((topic = PENDING.poll()) != null) {
                        sub.subscribe(topic.getBytes(StandardCharsets.UTF_8));
                    }
                    byte[] topicBytes = sub.recv(0);
                    if (topicBytes == null) continue;           // timeout: volta a checar PENDING
                    byte[] payload = sub.recv(0);
                    Instant received = Instant.now();
                    String channel = new String(topicBytes, StandardCharsets.UTF_8);
                    // inscrição ZeroMQ é por prefixo; aqui exigimos canal exato
                    if (payload == null || !SUBSCRIBED.contains(channel)) continue;
                    Map<String, Object> msg = MAPPER.readValue(payload, new TypeReference<>() {});
                    Instant sent = Instant.ofEpochMilli(((Number) msg.get("timestamp")).longValue());
                    System.out.printf(
                        "[%s] [%s] << MENSAGEM canal=%s autor=%s enviada_em=%s recebida_em=%s texto=\"%s\"%n",
                        received, USER, channel, msg.get("user"), sent, received, msg.get("message"));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, "subscriber");
        t.setDaemon(true);
        t.start();
    }

    static void subscribeToOneMore(List<String> channels) {
        List<String> candidates = new ArrayList<>(channels);
        candidates.removeAll(SUBSCRIBED);
        if (candidates.isEmpty()) return;
        String ch = candidates.get(RND.nextInt(candidates.size()));
        SUBSCRIBED.add(ch);   // primeiro o filtro, depois a inscrição
        PENDING.add(ch);
        log("++ INSCRITO", ch + " (total=" + SUBSCRIBED.size() + ")");
    }

    // bot 
    static final String[] PHRASES = {
        "olá", "alguém por aí?", "testando", "bom dia",
        "mensagem de teste", "o que acharam?", "ping", "salve o tricolor paulista"
    };

    static String randomMessage(int i) {
        return PHRASES[RND.nextInt(PHRASES.length)] + " #" + i + "-" + RND.nextInt(1000);
    }

    public static void main(String[] args) throws Exception {
        connect();
        startSubscriber();

        // login (repete até dar certo)
        while (!ok(request("login", Map.of()))) Thread.sleep(2000);

        while (true) {
            List<String> channels = listChannels();
            if (channels == null) { Thread.sleep(2000); continue; }

            // menos de 5 canais -> cria um novo
            if (channels.size() < MIN_CHANNELS) {
                request("channel", Map.of("channel", String.format("canal-%04d", RND.nextInt(10000))));
                List<String> updated = listChannels();
                if (updated == null) { Thread.sleep(2000); continue; }
                channels = updated;
            }

            // inscrito em menos de 3 canais -> inscreve em mais um
            if (SUBSCRIBED.size() < MIN_SUBSCRIPTIONS) subscribeToOneMore(channels);

            if (channels.isEmpty()) { Thread.sleep(1000); continue; }

            // escolhe um canal e publica 10 mensagens, 1 por segundo
            String channel = channels.get(RND.nextInt(channels.size()));
            for (int i = 1; i <= MSGS_PER_ROUND; i++) {
                request("publish", Map.of("channel", channel, "message", randomMessage(i)));
                Thread.sleep(1000);
            }
        }
    }
}
