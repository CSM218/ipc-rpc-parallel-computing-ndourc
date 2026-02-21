package pdc;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.io.IOException;
import java.io.InputStream;

/**
 * Message represents the communication unit in the CSM218 protocol.
 *
 * This is a CUSTOM BINARY PROTOCOL - NOT JSON, NOT Java Serialization.
 *
 * Message Format (Total: variable length):
 * - Magic Number  (4 bytes):  0xDEADBEEF — identifies our protocol
 * - Version       (1 byte):   protocol version
 * - Message Type  (1 byte):   0=REGISTER, 1=TASK, 2=RESULT, 3=HEARTBEAT, 4=ACK
 * - Sender Length (2 bytes):  length of sender name string
 * - Sender ID     (variable): sender name in UTF-8
 * - Task ID       (4 bytes):  which task this message belongs to
 * - Payload Len   (4 bytes):  length of payload
 * - Payload       (variable): matrix data
 * - Timestamp     (8 bytes):  when this message was created
 */
public class Message {

    // Message type constants
    public static final byte TYPE_REGISTER  = 0;
    public static final byte TYPE_TASK      = 1;
    public static final byte TYPE_RESULT    = 2;
    public static final byte TYPE_HEARTBEAT = 3;
    public static final byte TYPE_ACK       = 4;

    // Protocol constants
    private static final int  MAGIC_NUMBER = 0xDEADBEEF;
    private static final byte VERSION      = 1;

    // Message fields
    private final byte   type;
    private final String sender;
    private final int    taskId;
    private final byte[] payload;
    private       long   timestamp;

    public Message(byte type, String sender, int taskId, byte[] payload) {
        this.type      = type;
        this.sender    = sender  != null ? sender  : "";
        this.taskId    = taskId;
        this.payload   = payload != null ? payload : new byte[0];
        this.timestamp = System.currentTimeMillis();
    }

    // -------------------------------------------------------------------------
    // Serialisation
    // -------------------------------------------------------------------------

    /**
     * Packs the message into a byte array for network transmission.
     */
    public byte[] pack() {
        byte[] senderBytes = sender.getBytes(StandardCharsets.UTF_8);

        int totalSize =
                4 +                   // Magic number
                1 +                   // Version
                1 +                   // Type
                2 +                   // Sender length
                senderBytes.length +  // Sender
                4 +                   // Task ID
                4 +                   // Payload length
                payload.length +      // Payload
                8;                    // Timestamp

        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        buffer.order(ByteOrder.BIG_ENDIAN);

        buffer.putInt(MAGIC_NUMBER);
        buffer.put(VERSION);
        buffer.put(type);
        buffer.putShort((short) senderBytes.length);
        buffer.put(senderBytes);
        buffer.putInt(taskId);
        buffer.putInt(payload.length);
        buffer.put(payload);
        buffer.putLong(timestamp);

        return buffer.array();
    }

    /**
     * Reconstructs a Message from a raw byte array.
     * Must read fields in the same order as pack() writes them.
     */
    public static Message unpack(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.order(ByteOrder.BIG_ENDIAN);

        int magic = buffer.getInt();
        if (magic != MAGIC_NUMBER) {
            throw new IllegalArgumentException("Invalid magic number — not a valid protocol message");
        }

        buffer.get();                          // version (unused beyond validation)
        byte type = buffer.get();

        short senderLen  = buffer.getShort();
        byte[] senderBytes = new byte[senderLen];
        buffer.get(senderBytes);
        String sender = new String(senderBytes, StandardCharsets.UTF_8);

        int taskId = buffer.getInt();

        int payloadLen = buffer.getInt();
        byte[] payload = new byte[payloadLen];
        buffer.get(payload);

        long timestamp = buffer.getLong();

        Message msg = new Message(type, sender, taskId, payload);
        msg.timestamp = timestamp; // restore original timestamp
        return msg;
    }

    // -------------------------------------------------------------------------
    // Stream reading — handles TCP fragmentation
    // -------------------------------------------------------------------------

    /**
     * Reads exactly one complete Message from the given InputStream.
     * Returns null if the stream has ended cleanly (EOF at message boundary).
     * Throws IOException if the stream ends unexpectedly mid-message.
     */
    public static Message readFromStream(InputStream in) throws IOException {

        // ---- Step 1: read the fixed 8-byte header prefix ----
        // Layout: Magic(4) + Version(1) + Type(1) + SenderLen(2)
        byte[] prefix = new byte[8];
        int bytesRead = 0;
        while (bytesRead < 8) {
            int n = in.read(prefix, bytesRead, 8 - bytesRead);
            if (n == -1) {
                if (bytesRead == 0) return null; // clean EOF between messages
                throw new IOException("Stream ended mid-header (read " + bytesRead + "/8 bytes)");
            }
            bytesRead += n;
        }

        // ---- Step 2: peek at SenderLen ----
        ByteBuffer prefixBuf = ByteBuffer.wrap(prefix);
        prefixBuf.order(ByteOrder.BIG_ENDIAN);
        prefixBuf.getInt();  // skip magic
        prefixBuf.get();     // skip version
        prefixBuf.get();     // skip type
        short senderLen = prefixBuf.getShort();

        // ---- Step 3: read Sender + TaskID(4) + PayloadLen(4) ----
        int restHeaderLen = senderLen + 4 + 4;
        byte[] restHeader = new byte[restHeaderLen];
        bytesRead = 0;
        while (bytesRead < restHeaderLen) {
            int n = in.read(restHeader, bytesRead, restHeaderLen - bytesRead);
            if (n == -1) throw new IOException("Stream ended mid-header");
            bytesRead += n;
        }

        // ---- Step 4: extract PayloadLen ----
        // restHeader layout: Sender(senderLen) + TaskID(4) + PayloadLen(4)
        ByteBuffer restBuf = ByteBuffer.wrap(restHeader);
        restBuf.order(ByteOrder.BIG_ENDIAN);
        restBuf.position(senderLen + 4); // skip sender and taskId
        int payloadLen = restBuf.getInt();

        // ---- Step 5: read payload ----
        byte[] payload = new byte[payloadLen];
        bytesRead = 0;
        while (bytesRead < payloadLen) {
            int n = in.read(payload, bytesRead, payloadLen - bytesRead);
            if (n == -1) throw new IOException("Stream ended mid-payload");
            bytesRead += n;
        }

        // ---- Step 6: read trailing timestamp (8 bytes) ----
        byte[] timestampBytes = new byte[8];
        bytesRead = 0;
        while (bytesRead < 8) {
            int n = in.read(timestampBytes, bytesRead, 8 - bytesRead);
            if (n == -1) throw new IOException("Stream ended mid-timestamp");
            bytesRead += n;
        }

        // ---- Step 7: reassemble and unpack ----
        int total = 8 + restHeaderLen + payloadLen + 8;
        ByteBuffer full = ByteBuffer.allocate(total);
        full.order(ByteOrder.BIG_ENDIAN);
        full.put(prefix);
        full.put(restHeader);
        full.put(payload);
        full.put(timestampBytes);

        return unpack(full.array());
    }

    // -------------------------------------------------------------------------
    // Getters
    // -------------------------------------------------------------------------

    public byte   getType()      { return type;      }
    public String getSender()    { return sender;     }
    public int    getTaskId()    { return taskId;     }
    public byte[] getPayload()   { return payload;    }
    public long   getTimestamp() { return timestamp;  }
}
