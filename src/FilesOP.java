import java.io.*;
import java.net.Socket;
import java.util.*;

public class FilesOP {

    public static void main(String[] args) {
        for (Map.Entry<Integer, Vertex> e : readFiles(new File("com-amazon.ungraph.txt")).entrySet()) {
            System.out.println(e.getKey());
            System.out.println(e.getValue());
        }
    }

    // Return all the files within given directory (includes sub-directory)
    public static List<String> listFiles(String dirName) {

        List<String> result = new ArrayList<>();
        File curDir = new File(dirName);
        listFilesHelper(curDir, "", result);

        return result;
    }

    // Helper method to get filenames
    private static void listFilesHelper(File file, String dirName, List<String> result) {

        File[] fileList = file.listFiles();
        if (fileList != null) {
            for (File f : fileList) {
                if (f.isDirectory())
                    listFilesHelper(f, dirName + f.getName() + "/", result);
                if (f.isFile()) {
                    result.add(dirName + f.getName());
                }
            }
        }
    }

    public static HashMap<Integer, Vertex> readFiles(File file) {
        HashMap<Integer, Vertex> graph = new HashMap<>();
        try (
                BufferedReader buffer = new BufferedReader(new FileReader(file))
        ) {
            String line;
            while ((line = buffer.readLine()) != null) {
                if (line.startsWith("#"))
                    continue;
                String[] vertex = line.split("\t");
                int vertex1 = Integer.parseInt(vertex[0]);
                int vertex2 = Integer.parseInt(vertex[1]);
                Vertex v = graph.getOrDefault(vertex1, new Vertex(vertex1));
                v.neighbors.add(vertex2);
                if (!graph.containsKey(vertex1)) {
                    graph.put(vertex1, v);
                }

                // If undirected graph
//                list = graph.getOrDefault(vertex2, new ArrayList<>());
//                list.add(vertex1);
//                if (!graph.containsKey(vertex2)) {
//                    graph.put(vertex2, list);
//                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return graph;
    }

    // Delete the given file
    public static boolean deleteFile(String fileName) {
        File file = new File(fileName);
        return file.delete();
    }

    // Return a thread for sending file purpose
    public static Thread sendFile(File file, Socket socket) {
        return new SendFileThread(file, socket);
    }

    // Thread class for sending file purpose
    static class SendFileThread extends Thread {
        File file;
        Socket socket;

        public SendFileThread(File file, Socket socket) {
            this.file = file;
            this.socket = socket;
        }

        @Override
        public void run() {

            // Read file buffer
            byte[] buffer = new byte[2048];
            try (
                    DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
                    DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                    DataInputStream sktResponse = new DataInputStream(socket.getInputStream())
            ) {
                //Sending file size to the server
                dos.writeLong(file.length());

                //Sending file data to the server
                int read;
                while ((read = dis.read(buffer)) > 0)
                    dos.write(buffer, 0, read);

                // wait for the server to response
                String res = sktResponse.readUTF();
                if (res.equals("Received")) {
                    System.out.println("Put the file successfully");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
