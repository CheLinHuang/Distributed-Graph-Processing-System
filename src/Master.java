import java.io.IOException;
import java.net.ServerSocket;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

public class Master extends Thread {
    // for master, it has to maintain a jobQueue to ensure total ordering
    static Queue<String> jobQueue = new LinkedBlockingQueue<>();
    // metadata for the current files in the SDFS
    static Map<String, long[]> fileList = new HashMap<>();
    static Map<String, List<String>> fileReplica = new HashMap<>();
    // metadata for the graph processing task in SAVA
    static List<String> taskInfo = new ArrayList<>();
    static Integer iteration = 1;
    static String workers = "";
    static Map<Integer, Vertex> graph = new HashMap<>();
    static Map<Integer, String> partition = new HashMap<>();


    public void run() {

        System.out.println("Master Server Established! @ port#: " + Daemon.masterPortNumber);
        boolean listening = true;

        // Keep listening for incoming file related request
        try (ServerSocket serverSocket = new ServerSocket(Daemon.masterPortNumber)) {

            // Accept socket connection and create new thread
            while (listening)
                new MasterThread(serverSocket.accept()).start();

        } catch (IOException e) {
            System.err.println("Could not listen to port " + Daemon.masterPortNumber);
            System.exit(-1);
        }
    }

    public static void clearGraphTask() {
        taskInfo.clear();
        iteration = 1;
        workers = "";
        graph.clear();
        partition.clear();
        MasterThreadHelper.masterSynchronization();
    }
}
