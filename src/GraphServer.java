import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class GraphServer extends Thread {

    static final HashMap<Integer, Vertex> graph = new HashMap<>();
    static final HashMap<Integer, List<Double>> incoming = new HashMap<>();
    static final HashMap<Integer, String> partition = new HashMap<>();
    static final HashMap<String, HashMap<Integer, List<Double>>> outgoing = new HashMap<>();
    static final List<HashMap<Integer, List<Double>>> incomeCache = new ArrayList<>();
    static int vms;
    static int iterations;
    static double threshold;
    static boolean iterationDone = true;
    static boolean isInitialized = false;
    static GraphApplication graphApplication;

    public void run() {
        boolean listening = true;

        // Keep listening for incoming graph related request
        try (ServerSocket serverSocket = new ServerSocket(Daemon.graphPortNumber)) {

            // Accept socket connection and create new thread
            while (listening)
                new GraphServerThread(serverSocket.accept()).start();

        } catch (Exception e) {
            System.err.println("Could not listen to port " + Daemon.graphPortNumber);
            System.exit(-1);
        }
    }
}
