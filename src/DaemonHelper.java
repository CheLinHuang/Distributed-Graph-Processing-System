import java.io.*;
import java.net.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class DaemonHelper {

    public static void joinGroup(boolean isIntroducer, boolean isMaster) {

        // As required, wipes all the files stored in the SDFS system on this node
        for (String s : FilesOP.listFiles("../SDFS/"))
            FilesOP.deleteFile("../SDFS/" + s);

        DatagramSocket clientSocket = null;

        // try until socket create correctly
        while (clientSocket == null) {
            try {
                clientSocket = new DatagramSocket();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        byte[] sendData = (Daemon.ID + (isMaster? "#M": "#W")).getBytes();
        // send join request to each introducer
        for (String hostName : Daemon.hostNames) {
            try {
                InetAddress IPAddress = InetAddress.getByName(hostName);
                DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length,
                        IPAddress, Daemon.joinPortNumber);
                clientSocket.send(sendPacket);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        byte[] receiveData = new byte[1024];
        DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);

        // wait for the server's response
        try {
            clientSocket.setSoTimeout(2000);
            clientSocket.receive(receivePacket);
            String response = new String(receivePacket.getData(), 0, receivePacket.getLength());
            clientSocket.receive(receivePacket);
            String master = new String(receivePacket.getData(), 0, receivePacket.getLength());
            // process the membership list that the first introducer response and ignore the rest
            String[] members = response.split("%");
            for (String member : members) {
                String[] memberDetail = member.split("/");
                long[] memberStatus = {Long.parseLong(memberDetail[1]),
                        Long.parseLong(memberDetail[2]), System.currentTimeMillis()};
                Daemon.membershipList.put(memberDetail[0], memberStatus);
                int hashValue = Hash.hashing(memberDetail[0], 8);
                Daemon.hashValues.put(hashValue, memberDetail[0]);
                if (Integer.parseInt(memberDetail[2]) == Daemon.MASTER) {
                    Daemon.masterList.put(memberDetail[0], hashValue);
                }
            }
            Daemon.master = master;

            writeLog("JOIN!", Daemon.ID);
            updateNeighbors();
            synchronized (Daemon.membershipList) {
                if (!Daemon.membershipList.containsKey(Daemon.master))
                    masterElection();
            }
            try {
                DaemonHelper.checkReplica();
            } catch (Exception e) {
                e.printStackTrace();
            }
            // when the node is newly join the cluster,
            // always execute moveReplica
            // moveReplica(true);

        } catch (SocketTimeoutException e) {

            if (!isIntroducer) {
                System.err.println("All introducers are down!! Cannot join the group.");
                System.exit(1);
            } else {
                // This node might be the first introducer,
                // keep executing the rest of codes
                System.out.println("You might be the first introducer!");

                // put itself to the membership list
                long[] memberStatus = {0, isMaster? Daemon.MASTER: Daemon.WORKER, System.currentTimeMillis()};
                Daemon.membershipList.put(Daemon.ID, memberStatus);
                int hashValue = Hash.hashing(Daemon.ID, 8);
                Daemon.hashValues.put(hashValue, Daemon.ID);
                if (isMaster) {
                    Daemon.masterList.put(Daemon.ID, hashValue);
                    Daemon.master = Daemon.ID;
                    System.out.println("=============================");
                    System.out.println("New Master: " + Daemon.master);
                    // masterElection();
                }
                writeLog("JOIN", Daemon.ID);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void checkReplica() throws Exception {
        // checkReplica must be run after the completion of master election
        if (!Daemon.ID.equals(Daemon.master)) return;
        for (String sdfsFile: Master.fileReplica.keySet()) {
            System.out.println("Start to move " + sdfsFile + "...");
            List<String> oldTargetNodes = Master.fileReplica.get(sdfsFile);
            List<String> newTargetNodes =
                    Hash.getTargetNode(Hash.hashing(sdfsFile, 8));
            // transfer files to the new target nodes
            List<String> fileSinks = new ArrayList<>();
            for (String newTargetNode: newTargetNodes) {
                if (!oldTargetNodes.contains(newTargetNode)) {
                    fileSinks.add(newTargetNode);
                }
            }
            if (fileSinks.size() > 0) {
                Socket srcSocket = null;
                DataOutputStream srcOut = null;
                DataInputStream srcIn = null;
                for (String oldTargetNode: oldTargetNodes) {
                    try {
                        srcSocket = new Socket(
                                oldTargetNode.split("#")[1], Daemon.filePortNumber);
                        srcOut = new DataOutputStream(srcSocket.getOutputStream());
                        srcIn = new DataInputStream(srcSocket.getInputStream());
                        break;
                    } catch (Exception e) {
                        // do nothing, since the old target node might fail
                    }
                }
                List<Socket> sinkSockets = new ArrayList<>();
                List<DataOutputStream> sinkOut = new ArrayList<>();
                List<DataInputStream> sinkIn = new ArrayList<>();

                for (String fileSink: fileSinks) {
                    System.out.println("Send replica to " + fileSink.split("#")[1]);
                    Socket sinkSocket = new Socket(
                            fileSink.split("#")[1], Daemon.filePortNumber);
                    sinkSockets.add(sinkSocket);
                    sinkOut.add(new DataOutputStream(sinkSocket.getOutputStream()));
                    sinkIn.add(new DataInputStream(sinkSocket.getInputStream()));
                }

                srcOut.writeUTF("GET REPLICA");
                srcOut.writeUTF(sdfsFile);
                long fileTimeStamp = srcIn.readLong();
                srcOut.writeUTF("RESUME");

                for (DataOutputStream out: sinkOut) {
                    out.writeUTF("PUT REPLICA");
                    out.writeUTF(sdfsFile);
                    out.writeUTF("KEEP");
                    out.writeLong(fileTimeStamp);
                }
                MasterThreadHelper.transferFile(srcIn, sinkOut);
                for (DataInputStream in: sinkIn) {
                    in.readUTF();
                }
                System.out.println("Moving " + sdfsFile + " done!");
            }
            System.out.println("Start to delete " + sdfsFile + " on old nodes...");
            for (String oldTargetNode: oldTargetNodes) {
                if (!newTargetNodes.contains(oldTargetNode)) {
                    try {
                        Socket socket = new Socket(
                                oldTargetNode.split("#")[1], Daemon.filePortNumber);
                        DataOutputStream socketOut = new DataOutputStream(socket.getOutputStream());
                        DataInputStream socketIn = new DataInputStream(socket.getInputStream());
                        socketOut.writeUTF("DELETE REPLICA");
                        socketOut.writeUTF(sdfsFile);
                        socketIn.readUTF();
                    } catch (Exception e) {
                        // do nothing, since the old target node might fail
                    }
                }
            }
            System.out.println("Delete file: " + sdfsFile + " done!");
            Master.fileReplica.put(sdfsFile, newTargetNodes);
        }
    }

    public static void masterElection() {
        // we elect the master candidate with highest hash value as the master
        System.out.println("Doing master election...");
        synchronized (Daemon.masterList) {
            synchronized (Daemon.master) {
                int max = Integer.MIN_VALUE;
                System.out.println("Master List:");
                for (String key : Daemon.masterList.keySet()) {
                    System.out.println(key);
                    if (Daemon.masterList.get(key) > max) {
                        max = Daemon.masterList.get(key);
                        Daemon.master = key;
                    }
                }
                /*
                String oldMaster = Daemon.master;
                if (!Daemon.master.equals(oldMaster) && oldMaster.equals(Daemon.ID)) {
                    System.out.println("SYNC_PUSH");
                    try (Socket socket = new Socket(Daemon.master.split("#")[1], Daemon.masterPortNumber);
                         DataOutputStream socketOut = new DataOutputStream(socket.getOutputStream())
                    ) {
                        socketOut.writeUTF("SYNC_PUSH");
                        socketOut.writeInt(Master.fileList.size());
                        for (String key: Master.fileList.keySet())
                            socketOut.writeUTF(key + "#" + Arrays.toString(Master.fileList.get(key)));
                        socketOut.writeInt(Master.fileReplica.size());
                        for (String key: Master.fileReplica.keySet())
                            socketOut.writeUTF(key + "_" + Master.fileReplica.get(key).toString());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }*/
                if (Daemon.masterList.size() == 0)
                    Daemon.master = "None";
                System.out.println("=============================");
                System.out.println("New Master: " + Daemon.master);
            }
        }
    }

    public static void updateNeighbors() {

        synchronized (Daemon.membershipList) {
            synchronized (Daemon.neighbors) {
                List<String> oldNeighbors = new ArrayList<>(Daemon.neighbors);

                Daemon.neighbors.clear();

                Integer currentHash = Daemon.myHashValue;
                // get the predecessors
                for (int i = 0; i < 2; i++) {
                    currentHash = Daemon.hashValues.lowerKey(currentHash);
                    // since we are maintaining a virtual ring, if lower key is null,
                    // it means that we are at the end of the list
                    if (currentHash == null) {
                        currentHash = Daemon.hashValues.lastKey();
                    }
                    if (!currentHash.equals(Daemon.myHashValue) &&
                            !Daemon.neighbors.contains(Daemon.hashValues.get(currentHash))) {
                        try {
                            Daemon.neighbors.add(Daemon.hashValues.get(currentHash));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
                // get the successors
                currentHash = Daemon.myHashValue;
                for (int i = 0; i < 2; i++) {
                    currentHash = Daemon.hashValues.higherKey(currentHash);
                    if (currentHash == null) {
                        currentHash = Daemon.hashValues.firstKey();
                    }

                    if (!currentHash.equals(Daemon.myHashValue) &&
                            !Daemon.neighbors.contains(Daemon.hashValues.get(currentHash))) {
                        try {
                            Daemon.neighbors.add(Daemon.hashValues.get(currentHash));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }

                // check if the neighbors are changed
                // if yes, the files stored on this node might need to be moved
                // to maintain the consistency of the Chord-like ring
                Daemon.neighborUpdated = false;
                if (oldNeighbors.size() != Daemon.neighbors.size()) {
                    Daemon.neighborUpdated = true;
                } else {
                    for (int i = 0; i < oldNeighbors.size(); i++) {
                        if (!oldNeighbors.get(i).equals(Daemon.neighbors.get(i))) {
                            Daemon.neighborUpdated = true;
                            break;
                        }
                    }
                }

                // for debugging
                System.out.println("print neighbors......");
                for (String neighbor : Daemon.neighbors) {
                    System.out.println(neighbor);
                }

                // Update timestamp for neighbors
                // Since the daemon will only receive heartbeat from its old neighbors,
                // it will only update the timestamp of its old neighbor. Once we update our neighbors,
                // we need to update its timestamp to the current time, otherwise, the monitor thread will
                // immediately regards the new neighbor is timeout.
                for (String neighbor : Daemon.neighbors) {
                    long[] memberStatus = {Daemon.membershipList.get(neighbor)[0],
                            Daemon.membershipList.get(neighbor)[1], System.currentTimeMillis()};
                    Daemon.membershipList.put(neighbor, memberStatus);

                }
            }
        }
    }

    public static void writeLog(String action, String nodeID) {

        // write logs about action happened to the nodeID into log
        Daemon.fileOutput.println(LocalDateTime.now().toString() + " \"" + action + "\" " + nodeID);
        Daemon.fileOutput.flush();
    }
}
