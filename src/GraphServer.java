import java.net.ServerSocket;
import java.util.HashMap;
import java.util.List;

public class GraphServer {

    static HashMap<Integer, Vertex> graph;
    static HashMap<Integer, List<Double>> incoming;
    static HashMap<Integer, String> partition;
    static HashMap<String, HashMap<Integer, List<Double>>> outgoing;
    static int gatherCount = 0;
    static int vms;
    static boolean iterationDone = false;
    static boolean isFinish = false;
    static boolean isInitialized = false;
    static boolean needResend = false;
    static boolean isPageRank;
    static double damping;
    static int N;

    public void run() {
        boolean listening = true;

        // Keep listening for incoming file related request
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