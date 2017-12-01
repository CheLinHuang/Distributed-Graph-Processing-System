import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class FilesOP {

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

    // Delete the given file
    public static boolean deleteFile(String fileName) {
        File file = new File(fileName);
        return file.delete();
    }

    public static void sendFile(DataOutputStream socketOut, File file) throws Exception {

        byte[] buffer = new byte[2048];
        DataInputStream data = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
        socketOut.writeLong(file.length());
        int bytes;
        while ((bytes = data.read(buffer)) > 0)
            socketOut.write(buffer, 0, bytes);
    }


    /*
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
                if (res.equals("DONE")) {
                    System.out.println("Put the file successfully");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }*/
}
