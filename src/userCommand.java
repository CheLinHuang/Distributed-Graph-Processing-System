import java.io.*;
import java.net.Socket;
import java.util.*;

public class userCommand {

    public static void putFile(String[] cmdParts) {

        if (cmdParts.length != 3) {
            System.out.println("Unsupported command format!");
            System.out.println("To put a file into the SDFS");
            System.out.println("Please enter \"put srcFileName tgtFileName\"");
            return;
        }

        String srcFileName = cmdParts[1];
        String tgtFileName = cmdParts[2];
        DaemonHelper.writeLog("put file to", Daemon.master.split("#")[1]);

        // Open the local file
        File file = new File(srcFileName);
        if (!file.exists()) {
            DaemonHelper.writeLog("Local file not exist", srcFileName);
            System.out.println("Local file not exist!");
        } else {

            boolean forceWrite = false;
            while (true) {
                try {
                    sendFile("PUT", new ArrayList<>(), file, tgtFileName, forceWrite);
                    // if the file is successfully sent, break the while loop
                    break;
                } catch (Exception e) {
                    // if exception occurs, wait for a while and resend the file
                    forceWrite = true;
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ex) {
                        // do nothing
                    }
                }
            }
        }
    }

    public static void sendFile(
            String operation, List<String> extraInfo,
            File srcFile, String tgtFileName, boolean forceWrite) throws Exception {

        Socket socket = new Socket(
                Daemon.master.split("#")[1], Daemon.masterPortNumber);
        DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
        DataInputStream in = new DataInputStream(socket.getInputStream());
        dos.writeUTF(operation);
        dos.writeUTF(tgtFileName);
        // extraInfo contains additional information about graph
        if (extraInfo.size() > 0) {
            dos.writeInt(extraInfo.size());
            for (String info : extraInfo)
                dos.writeUTF(info);
        }
        String response = in.readUTF();
        DaemonHelper.writeLog("Server response", response);

        //Thread t = null;
        if (response.equals("ACCEPT") || forceWrite) {
            // Accept the put file request
            FilesOP.sendFile2(srcFile, socket);

        } else if (response.equals("CONFIRM")) {

            // Require confirmation to put file
            System.out.println("Are you sure to send the file? (y/n)");
            BufferedReader StdIn = new BufferedReader(new InputStreamReader(System.in));

            // Require confirmation within 30 sec
            long startTime = System.currentTimeMillis();
            while (((System.currentTimeMillis() - startTime) < 30000) && !StdIn.ready()) {
                try {
                    Thread.sleep(200);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            if (StdIn.ready()) {
                boolean repeat = true;
                while (repeat) {
                    String cmd = StdIn.readLine().toLowerCase();
                    switch (cmd) {
                        case "y":
                            dos.writeUTF("Y");
                            DaemonHelper.writeLog("Allow file update within 1 min", tgtFileName);
                            //t = FilesOP.sendFile(file, socket);
                            FilesOP.sendFile2(srcFile, socket);
                            repeat = false;
                            break;
                        case "n":
                            dos.writeUTF("N");
                            DaemonHelper.writeLog("Reject file update within 1 min", tgtFileName);
                            repeat = false;
                            // do nothing
                            break;
                        default:
                            System.out.println("Unsupported command!");
                            System.out.println("Are you sure to send the file? (y/n)");
                    }
                }
            } else {
                dos.writeUTF("N");
                DaemonHelper.writeLog("No response for confirmation", tgtFileName);
                System.out.println("No response! Update aborted!");
            }
        }
        if (in.readUTF().equals("DONE"))
            System.out.println("Put the file successfully!");
        else System.out.println("Update aborted!");
        DaemonHelper.writeLog("put complete", tgtFileName);
        socket.close();
    }


    public static void listFile(String[] cmdParts) {

        if (cmdParts.length != 2) {
            System.out.println("Unsupported command format!");
            System.out.println("To list a file on the SDFS");
            System.out.println("Please enter \"ls sdfsfilename\"");
            return;
        }

        String sdfsFileName = cmdParts[1];
        String fileServer = Daemon.master.split("#")[1];

        try {
            // Connect to server
            Socket socket = new Socket(fileServer, Daemon.masterPortNumber);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            DataInputStream in = new DataInputStream(socket.getInputStream());
            out.writeUTF("LIST");
            out.writeUTF(sdfsFileName);

            socket.setSoTimeout(2000);
            String response = in.readUTF();
            if (response.equals("EMPTY")) {
                System.out.println("File doesn't exist");
            } else {
                String[] nodes = response.split("#");
                System.out.println(sdfsFileName + " is stored in the following nodes:");
                for (String node : nodes) {
                    System.out.println(node);
                }
                System.out.println("==================================");
            }
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
            listFile(cmdParts);
        }
    }

    public static void getFile(String[] cmdParts) {
        if (cmdParts.length != 3) {
            System.out.println("Unsupported command format!");
            System.out.println("To get a file from the SDFS");
            System.out.println("Please enter \"get sdfsfilename localfilename\"");
            return;
        }

        String sdfsfilename = cmdParts[1];
        String localfilename = cmdParts[2];
        String fileServer = Daemon.master.split("#")[1];
        File localFile = new File(localfilename);
        try {
            // Connect to server
            Socket socket = new Socket(fileServer, Daemon.masterPortNumber);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            DataInputStream in = new DataInputStream(socket.getInputStream());
            out.writeUTF("GET");
            out.writeUTF(sdfsfilename);

            String response = in.readUTF();
            System.out.println("Server Response: " + response);

            if (response.equals("FILE_FOUND")) {
                System.out.println("File is found! Start to fetch file!");
                BufferedOutputStream fileOutputStream = new BufferedOutputStream(
                        new FileOutputStream(localFile));

                long fileSize = in.readLong();
                byte[] buffer = new byte[Daemon.bufferSize];
                int bytes;
                while (fileSize > 0 && (bytes = in.read(buffer, 0,
                        (int) Math.min(Daemon.bufferSize, fileSize))) != -1) {
                    fileOutputStream.write(buffer, 0, bytes);
                    fileSize -= bytes;
                }
                fileOutputStream.close();
                System.out.println("get the file successfully");
                DaemonHelper.writeLog("Get file completed", sdfsfilename);
            } else {
                System.out.println("File not found!");
                DaemonHelper.writeLog("File not found", sdfsfilename);
            }
            socket.close();
        } catch (Exception e) {
            localFile.delete();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {
                // do nothing
            }
            getFile(cmdParts);
        }
    }

    public static void deleteFile(String[] cmdParts) {
        if (cmdParts.length != 2) {
            System.out.println("Unsupported command format!");
            System.out.println("To delete a file on the SDFS");
            System.out.println("Please enter \"delete sdfsfilename\"");
            return;
        }

        String sdfsfilename = cmdParts[1];
        String fileServer = Daemon.master.split("#")[1];

        try {
            Socket socket = new Socket(fileServer, Daemon.masterPortNumber);
            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
            DataInputStream dis = new DataInputStream(socket.getInputStream());
            dos.writeUTF("DELETE");
            dos.writeUTF(sdfsfilename);
            if (dis.readUTF().equals("DONE"))
                System.out.println("Delete the file successfully!");
            else System.out.println("File doesn't exist!");
            socket.close();
        } catch (Exception e) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {
                // do nothing
            }
            deleteFile(cmdParts);
        }
    }

    public static void savaGraph(String[] cmdParts) {
        // System.out.println("Enter \"sava task(pagerank/sssp) taskparams localgraphfile outputsdfsfilename");
        // taskparams are:
        // [damping factor, numOfIteration(Optional)] for PageRank
        // [sourceVertex, numOfIteration (Optional)] for Single Source Shortest Path

        // handle invalid input
        String task = cmdParts[1].toLowerCase();
        List<String> params = new ArrayList<>();
        params.add(Daemon.ID);

        if (!task.equals("pagerank") && !task.equals("sssp")) {
            System.out.println("Unsupported task!");
            System.out.println("Please select task from [pagerank, sssp]");
            return;
        }

        if (cmdParts.length != 6) {
            System.out.println("Unsupported command format");
            System.out.println(
                    "Please enter \"sava pagerank\\sssp dampingFactor "
                            + "Iteration\\Threshold localgraphfile outputsdfsfilename\"");
            return;
        }

        for (int i = 1; i < cmdParts.length - 2; i++) {
            params.add(cmdParts[i].toLowerCase());
        }

        String localGraphFile = cmdParts[cmdParts.length - 2];

        File file = new File(localGraphFile);
        if (!file.exists()) {
            System.out.println("Local file not exist!");
            return;
        }

        if (localGraphFile.indexOf("/") != -1) {
            String[] temp = localGraphFile.split("/");
            localGraphFile = temp[temp.length - 1];
            System.out.println(localGraphFile);
        }

        // input is valid, starts to put the graph into the SDFS
        String outputFile = cmdParts[cmdParts.length - 1];
        params.add(outputFile);

        boolean forceWrite = false;
        while (true)
        try {
            sendFile("SAVA", params, file, localGraphFile, forceWrite);
            break;
        } catch (Exception e) {
            forceWrite = true;
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                // do nothing
            }
        }
    }
}
