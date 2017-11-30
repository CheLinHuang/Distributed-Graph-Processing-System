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
    static boolean isPageRank = false;
    static double damping;
    static double threshold;
    static int N;

    public static void main(String[] args) {
    //public void run() {
        boolean listening = true;

        // Keep listening for incoming file related request
        try (ServerSocket serverSocket = new ServerSocket(12345)) {

            // Accept socket connection and create new thread
            while (listening)
                new GraphServerThread(serverSocket.accept()).start();

        } catch (Exception e) {
            System.err.println("Could not listen to port " + 12345);
            System.exit(-1);
        }
    }
}
