package bbs;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.time.Instant;
import java.util.*;

public class Client {
    static final ObjectMapper MAPPER = new ObjectMapper(new MessagePackFactory());
    static final String BROKER = env("BROKER_FRONTEND", "tcp://broker:5555");
    static final String USER = env("BOT_NAME", "bot");
    static final List<String> WANTED = Arrays.asList(env("WANTED_CHANNELS", "geral").split(","));
    static final int TIMEOUT_MS = 3000;

    static final ZContext ctx = new ZContext();
    static ZMQ.Socket sock;

    static String env(String k, String d) {
        String v = System.getenv(k);
        return (v == null || v.isBlank()) ? d : v;
    }

    static void log(String tag, Map<String, Object> m) {
        System.out.printf("[%s] [%s] %s %s%n", Instant.now(), USER, tag, m);
    }

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
            System.out.printf("[%s] [%s] !! timeout, reconectando%n", Instant.now(), USER);
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

    @SuppressWarnings("unchecked")
    static List<String> listChannels() throws Exception {
        Map<String, Object> rep = request("channels", Map.of());
        return ok(rep) ? (List<String>) rep.get("channels") : List.of();
    }

    public static void main(String[] args) throws Exception {
        connect();

        // 1) login (repete até dar certo)
        while (!ok(request("login", Map.of()))) Thread.sleep(2000);

        // 2) cria os canais desejados que ainda não existem
        List<String> existing = listChannels();
        for (String ch : WANTED) {
            if (!existing.contains(ch)) request("channel", Map.of("channel", ch));
        }

        // 3) lista periodicamente (útil para ver os servidores alternando)
        while (true) {
            Thread.sleep(10000);
            listChannels();
        }
    }
}
