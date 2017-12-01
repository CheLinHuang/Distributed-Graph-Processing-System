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
                            FilesOP.sendFile(sktOutput, file);
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
            }
            //release the resource
            socket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
