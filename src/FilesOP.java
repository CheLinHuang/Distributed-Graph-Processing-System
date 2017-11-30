import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.util.*;

public class FilesOP {

    public static void main(String[] args) {
        try {

            long time1 = System.currentTimeMillis();

            System.out.println(InetAddress.getLocalHost().getHostName());
            Socket socket = new Socket(InetAddress.getLocalHost().getHostName(), 12345);
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

            HashMap<Integer, Vertex> hm = readFiles(new File("../test.txt"));

            for (int i : hm.keySet()) {
                hm.get(i).value = 1.0; // / hm.size();
            }

            System.out.println("Done init");

            out.writeUTF("init");
            out.flush();
            out.writeUTF("PageRank");
            out.flush();
            out.writeDouble(0.85);
            out.flush();
            out.writeInt(hm.size());
            out.flush();
            System.out.println(in.readUTF());

            time1 = System.currentTimeMillis() - time1;

            out.writeUTF("ADD");
            out.flush();
            out.writeInt(hm.size());
            out.flush();
            for (Vertex e : hm.values()) {
                out.writeObject(e);
            }
            out.flush();
            System.out.println(in.readUTF());

            long time = System.currentTimeMillis();

            out.writeUTF("neighbor info");
            out.flush();
            out.writeInt(0);
            out.flush();
            System.out.println(in.readUTF());


            int it = 20;
            while (it > 0) {
                out.writeUTF("ITERATION");
                out.flush();
                it--;
                System.out.println(in.readUTF());
            }

            out.writeUTF("TERMINATE");
            out.flush();

            int num = in.readInt();
            System.out.println(num);

            List<String> list = new ArrayList<>();
            
            while (num > 0) {
                int numm = in.readInt();
                double d = in.readDouble();
                Formatter ff = new Formatter();
                list.add(numm + "," + d);
                num--;
            }
            Collections.sort(list);
            File f = new File("result.txt");
            PrintWriter fs = new PrintWriter(f);
            for (String s : list)
                fs.println(s);
            fs.close();
            System.out.println(System.currentTimeMillis() - time + time1) ;

        } catch (Exception e) {
            e.printStackTrace();
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
                if (vertex1 == 1)
                    v.value = 0;
                else
                    v.value = Double.MAX_VALUE;
                v.neighbors.add(vertex2);
                if (!graph.containsKey(vertex1)) {
                    graph.put(vertex1, v);
                }

                // If undirected graph
                Vertex v2 = graph.getOrDefault(vertex2, new Vertex(vertex2));
                if (vertex2 == 1)
                    v2.value = 0;
                else
                    v2.value = Double.MAX_VALUE;
                if (!graph.containsKey(vertex2)) {
                    graph.put(vertex2, v2);
                }
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
