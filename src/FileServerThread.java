import java.io.*;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.List;

public class FileServerThread extends Thread {

    Socket socket;

    public FileServerThread(Socket socket) {
        this.socket = socket;
    }

    @Override
    public void run() {

        try (
                DataOutputStream sktOutput = new DataOutputStream(socket.getOutputStream());
                DataInputStream sktInput = new DataInputStream(socket.getInputStream())
        ) {

            String operation = "";
            String sdfsfilename = "";
            try{
                operation = sktInput.readUTF();
                sdfsfilename = sktInput.readUTF();
            } catch (Exception e) {
                return;
            }
            DaemonHelper.writeLog("##FileServer## Received Request: " + operation,
                    socket.getRemoteSocketAddress().toString());

            switch (operation) {

                case "PUT REPLICA": {

                    if (sktInput.readUTF().equals("KEEP")) {

                        File sdfsfile = new File("../SDFS/" + sdfsfilename);
                        try {
                            // Open the file in SDFS for writing
                            BufferedOutputStream fileOutputStream = new BufferedOutputStream(
                                    new FileOutputStream(sdfsfile));
                            long fileTimeStamp = sktInput.readLong();
                            long fileSize = sktInput.readLong();
                            System.out.println("Received File Size: " + fileSize);
                            byte[] buffer = new byte[Daemon.bufferSize];
                            int bytes;
                            while (fileSize > 0 &&
                                    (bytes = sktInput.read(buffer, 0, (int) Math.min(Daemon.bufferSize, fileSize))) != -1) {
                                fileOutputStream.write(buffer, 0, bytes);
                                fileSize -= bytes;
                            }
                            fileOutputStream.close();

                            FileServer.fileList.put(sdfsfilename, fileTimeStamp);
                            sktOutput.writeUTF("RECEIVED");
                            DaemonHelper.writeLog("Receive file successfully", "");
                        } catch (Exception e) {
                            sdfsfile.delete();
                            FileServer.fileList.remove(sdfsfilename);
                        }
                    }
                    break;
                }
                case "GET REPLICA": {
                    if (!FileServer.fileList.containsKey(sdfsfilename))
                        sktOutput.writeLong(-1);
                    else {
                        sktOutput.writeLong(FileServer.fileList.get(sdfsfilename));
                        String response = sktInput.readUTF();
                        if (response.equals("RESUME")) {
                            // Open the file in SDFS
                            File file = new File("../SDFS/" + sdfsfilename);
                            FilesOP.sendFile2(file, socket);
                            DaemonHelper.writeLog("get complete", sdfsfilename);
                        }
                    }
                    break;
                }
                case "DELETE REPLICA": {
                    if (FilesOP.deleteFile("../SDFS/" + sdfsfilename)) {
                        sktOutput.writeUTF("DONE");
                        FileServer.fileList.remove(sdfsfilename);
                    }
                    else
                        sktOutput.writeUTF("FILE_NOT_FOUND");
                    break;
                }
                case "LIST REPLICA": {
                    if (FileServer.fileList.containsKey(sdfsfilename))
                        sktOutput.writeUTF("FILE_FOUND");
                    else sktOutput.writeUTF("FILE_NOT_FOUND");
                    break;
                }
                /*
                case "fail replica": {
                    // Read filename from clientData.readUTF()
                    //String sdfsfilename = clientData.readUTF();
                    DaemonHelper.writeLog(sdfsfilename, "");

                    if (!new File("../SDFS/" + sdfsfilename).exists()) {

                        // If no replica, receive the replica immediately
                        out.writeUTF("Ready to receive");
                        BufferedOutputStream fileOutputStream = new BufferedOutputStream(
                                new FileOutputStream("../SDFS/" + sdfsfilename));

                        long fileSize = clientData.readLong();
                        byte[] buffer = new byte[Daemon.bufferSize];
                        int bytes;
                        while (fileSize > 0 && (bytes = clientData.read(buffer, 0, (int) Math.min(Daemon.bufferSize, fileSize))) != -1) {
                            fileOutputStream.write(buffer, 0, bytes);
                            fileSize -= bytes;
                        }
                        fileOutputStream.close();
                        out.writeUTF("Received");
                        DaemonHelper.writeLog("Receive Replica", "");

                    } else {

                        // Replica exist, no need to overwrite
                        DaemonHelper.writeLog("Replica Exist", "");
                        out.writeUTF("Replica Exist");
                    }
                    break;
                }

                case "delete": {
                    // Read filename from clientData.readUTF()
                    String sdfsfilename = clientData.readUTF();
                    DaemonHelper.writeLog(sdfsfilename, "");

                    FilesOP.deleteFile("../SDFS/" + sdfsfilename);

                    // Delete replica
                    int index = Daemon.neighbors.size() - 1;
                    for (int i = 0; index >= 0 && i < 2; i++) {
                        Socket replicaSocket = new Socket(Daemon.neighbors.get(index).split("#")[1], Daemon.filePortNumber);
                        DataOutputStream outPrint = new DataOutputStream(replicaSocket.getOutputStream());
                        outPrint.writeUTF("delete replica");
                        outPrint.writeUTF(sdfsfilename);
                        replicaSocket.close();
                        index--;
                    }
                    break;
                }

                case "ls": {
                    // Read filename from clientData.readUTF()
                    String sdfsFileName = clientData.readUTF();
                    DaemonHelper.writeLog(sdfsFileName, "");

                    String queryResult = "";
                    // query the file locally on the coordinator
                    if (new File("../SDFS/" + sdfsFileName).exists()) {
                        queryResult += Daemon.ID.split("#")[1] + "#";
                    }

                    // query the file on the neighbors of the coordinator
                    int j = Daemon.neighbors.size() - 1;
                    while (j >= 0) {
                        String tgtNode = Daemon.neighbors.get(j--).split("#")[1];
                        try {
                            // Connect to server
                            Socket lsSocket = new Socket(tgtNode, Daemon.filePortNumber);
                            DataOutputStream lsOut = new DataOutputStream(lsSocket.getOutputStream());
                            DataInputStream lsIn = new DataInputStream(lsSocket.getInputStream());

                            lsOut.writeUTF("ls replica");
                            lsOut.writeUTF(sdfsFileName);

                            lsSocket.setSoTimeout(1000);
                            String result = lsIn.readUTF();
                            if (!result.equals("Empty")) {
                                queryResult += result + "#";
                            }

                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    if (queryResult.isEmpty()) {
                        out.writeUTF("Empty");
                    } else {
                        queryResult = queryResult.substring(0, queryResult.length() - 1);
                        out.writeUTF(queryResult);
                    }
                    break;
                }
                case "ls replica": {
                    // check if the query file exists on the replica node
                    String sdfsFileName = clientData.readUTF();
                    DaemonHelper.writeLog(sdfsFileName, "");

                    if (new File("../SDFS/" + sdfsFileName).exists()) {
                        out.writeUTF(Daemon.ID.split("#")[1]);
                    } else {
                        out.writeUTF("Empty");
                    }
                    break;
                }
                case "get replica": {
                    String targetNode = clientData.readUTF();
                    List<String> fileList = FilesOP.listFiles("../SDFS/");
                    if (fileList.size() == 0) {
                        out.writeUTF("Empty");
                    } else {
                        for (String file : fileList) {
                            if (targetNode.equals(Hash.getServer(Hash.hashing(file, 8)))) {
                                out.writeUTF(file);
                                Thread t = FilesOP.sendFile(new File("../SDFS/" + file), socket);
                                t.start();
                                t.join();
                            }
                        }
                        out.writeUTF("Empty");
                    }
                }
                */
            }
            //release the resource
            socket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
