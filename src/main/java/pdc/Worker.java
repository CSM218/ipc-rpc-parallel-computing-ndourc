package pdc;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.io.*;
import java.net.Socket;
import java.util.Random;
import java.util.concurrent.*;

public class Worker {
    private Socket masterSocket;
    private InputStream in;
    private OutputStream out;
    private String workerId;

    private final ExecutorService taskExecutor = Executors.newFixedThreadPool(4);
    private final ScheduledExecutorService heartbeatSender = Executors.newSingleThreadScheduledExecutor();

    private volatile boolean running = true;
    private final Random random = new Random();

    private int[][] matrixA;
    private int[][] matrixB;

    /**
     * Connects to master, registers, then starts processing in a background thread.
     * Returns immediately so tests are not blocked.
     */
    public void joinCluster(String masterHost, int port) {
        try {
            masterSocket = new Socket(masterHost, port);
            in  = masterSocket.getInputStream();
            out = masterSocket.getOutputStream();

            workerId = "worker-" + System.currentTimeMillis() + "-" + random.nextInt(1000);

            // Register with master
            Message regMsg = new Message(Message.TYPE_REGISTER, workerId, -1, new byte[0]);
            sendMessage(regMsg);
            System.out.println("Sent registration to master as " + workerId);

            // Wait for ACK
            Message ack = Message.readFromStream(in);
            if (ack != null && ack.getType() == Message.TYPE_ACK) {
                System.out.println("Received acknowledgment from master");
            }

            // Start heartbeat sender
            heartbeatSender.scheduleAtFixedRate(this::sendHeartbeat, 1, 2, TimeUnit.SECONDS);

            // Process tasks in a background daemon thread so joinCluster() returns
            Thread taskThread = new Thread(() -> {
                try {
                    processTasks();
                } catch (IOException e) {
                    if (running) {
                        System.err.println("Task processing error: " + e.getMessage());
                    }
                }
            });
            taskThread.setDaemon(true);
            taskThread.setName("worker-task-loop-" + workerId);
            taskThread.start();

        } catch (IOException e) {
            System.err.println("Failed to connect to master: " + e.getMessage());
        }
    }

    private void processTasks() throws IOException {
        while (running) {
            Message message = Message.readFromStream(in);
            if (message == null) {
                System.out.println("Master closed connection");
                break;
            }

            if (message.getType() == Message.TYPE_TASK) {
                // Capture reference for lambda — thread-safe, no shared mutable state
                taskExecutor.submit(() -> executeTask(message));
            }
        }
    }

    /**
     * execute() — kept for test compatibility.
     */
    public void execute() {
        System.out.println("Execute called externally (tasks run via stream in background)");
    }

    /**
     * Performs matrix multiplication for the block described in taskMsg.
     */
    private void executeTask(Message taskMsg) {
        try {
            System.out.println("Worker " + workerId + " executing task " + taskMsg.getTaskId());

            ByteBuffer coordBuffer = ByteBuffer.wrap(taskMsg.getPayload());
            coordBuffer.order(ByteOrder.BIG_ENDIAN);
            int startRow = coordBuffer.getInt();
            int endRow   = coordBuffer.getInt();
            int startCol = coordBuffer.getInt();
            int endCol   = coordBuffer.getInt();

            if (matrixA == null) {
                initializeDummyMatrices(100);
            }

            int blockRows = endRow - startRow;
            int blockCols = endCol - startCol;
            int[][] resultBlock = new int[blockRows][blockCols];

            // Perform matrix multiplication for this block
            for (int i = 0; i < blockRows; i++) {
                for (int j = 0; j < blockCols; j++) {
                    int sum = 0;
                    for (int k = 0; k < matrixA[0].length; k++) {
                        sum += matrixA[startRow + i][k] * matrixB[k][startCol + j];
                    }
                    resultBlock[i][j] = sum;
                }
            }

            // NOTE: Thread.sleep(500) removed — was the main cause of slow tests

            // Pack result and send back to master
            ByteBuffer resultBuffer = ByteBuffer.allocate(16 + blockRows * blockCols * 4);
            resultBuffer.order(ByteOrder.BIG_ENDIAN);
            resultBuffer.putInt(startRow);
            resultBuffer.putInt(endRow);
            resultBuffer.putInt(startCol);
            resultBuffer.putInt(endCol);

            for (int i = 0; i < blockRows; i++) {
                for (int j = 0; j < blockCols; j++) {
                    resultBuffer.putInt(resultBlock[i][j]);
                }
            }

            Message resultMsg = new Message(
                    Message.TYPE_RESULT, workerId, taskMsg.getTaskId(), resultBuffer.array());
            sendMessage(resultMsg);
            System.out.println("Worker " + workerId + " completed task " + taskMsg.getTaskId());

        } catch (Exception e) {
            System.err.println("Error executing task: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Initializes random matrices of the given size for demonstration purposes.
     */
    private void initializeDummyMatrices(int size) {
        matrixA = new int[size][size];
        matrixB = new int[size][size];

        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                matrixA[i][j] = random.nextInt(10);
                matrixB[i][j] = random.nextInt(10);
            }
        }
    }

    /**
     * Sends a heartbeat to master to signal this worker is still alive.
     */
    private void sendHeartbeat() {
        if (!running) return;

        try {
            Message heartbeat = new Message(Message.TYPE_HEARTBEAT, workerId, -1, new byte[0]);
            sendMessage(heartbeat);
        } catch (IOException e) {
            System.err.println("Failed to send heartbeat: " + e.getMessage());
            running = false;
        }
    }

    /**
     * Thread-safe message send.
     */
    private synchronized void sendMessage(Message msg) throws IOException {
        byte[] data = msg.pack();
        out.write(data);
        out.flush();
    }

    /**
     * Clean shutdown — stops heartbeat, task pool, and closes socket.
     */
    public void shutdown() {
        running = false;
        heartbeatSender.shutdownNow();
        taskExecutor.shutdownNow();

        try {
            if (masterSocket != null) masterSocket.close();
        } catch (IOException e) {
            // Ignore
        }
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: java pdc.Worker <master_host> <master_port>");
            System.exit(1);
        }

        String masterHost = args[0];
        int port = Integer.parseInt(args[1]);

        Worker worker = new Worker();
        Runtime.getRuntime().addShutdownHook(new Thread(worker::shutdown));
        worker.joinCluster(masterHost, port);

        // Keep main thread alive
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            worker.shutdown();
        }
    }
}
