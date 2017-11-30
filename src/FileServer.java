import java.io.IOException;
import java.net.ServerSocket;
import java.util.*;

public class FileServer extends Thread {

    static Map<String, Long> fileList = new HashMap<>();

    public void run() {
        System.out.println("File Server Established @ port#: " + Daemon.filePortNumber);
        boolean listening = true;

        // Keep listening for incoming file related request
        try (ServerSocket serverSocket = new ServerSocket(Daemon.filePortNumber)) {

            // Accept socket connection and create new thread
            while (listening)
                new FileServerThread(serverSocket.accept()).start();

        } catch (IOException e) {
            System.err.println("Could not listen to port " + Daemon.filePortNumber);
            System.exit(-1);
        }
    }
}
