import java.net.ServerSocket;
import java.net.SocketAddress;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

public class FileServer extends Thread {

    static ReentrantLock lock = new ReentrantLock();
    static Queue<SocketAddress> putQueue = new LinkedBlockingQueue<>();

    public void run() {
        boolean listening = true;

        // Keep listening for incoming file related request
        try (ServerSocket serverSocket = new ServerSocket(Daemon.filePortNumber)) {

            // Accept socket connection and create new thread
            while (listening)
                new FileServerThread(serverSocket.accept()).start();

        } catch (Exception e) {
            System.err.println("Could not listen to port " + Daemon.filePortNumber);
            System.exit(-1);
        }
    }
}
