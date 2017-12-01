import java.io.*;
import java.net.Socket;
import java.util.*;

public class MasterThread extends Thread {

    Socket socket;
    final static int NUM_OF_REPLICA = 3;

    public MasterThread(Socket socket) {
        this.socket = socket;
    }

    @Override
    public void run() {

        try {

            DataInputStream clientData = new DataInputStream(socket.getInputStream());
            String operation = clientData.readUTF();

            if (operation.equals("SYNC")) {

                // for backup master to sync meta data of graph processing task
                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                Master.fileList = (HashMap<String, long[]>) ois.readObject();
                Master.fileReplica = (HashMap<String, List<String>>) ois.readObject();
                Master.taskInfo = (List<String>) ois.readObject();
                Master.graph = (HashMap<Integer, Vertex>) ois.readObject();

                /* for debugging
                System.out.println("Synchronization Done");
                for (String key: Master.fileList.keySet()) {
                    System.out.println(key + "::" + Arrays.toString(Master.fileList.get(key)));
                    System.out.println(key + "::" + Master.fileReplica.get(key).toString());
                }
                System.out.println(Master.taskInfo.toString());
                for (int key: Master.graph.keySet()) {
                    System.out.println(key + "::" + Master.graph.get(key));
                }
                */
                oos.writeUTF("DONE");
                oos.flush();

            } else {

                DataOutputStream clientOut = new DataOutputStream(socket.getOutputStream());
                String sdfsfilename = clientData.readUTF();
                String job = operation + "_" + sdfsfilename + "_" + System.currentTimeMillis();
                System.out.println("MASTER: Got " + operation +
                        " request from: " + socket.getRemoteSocketAddress().toString());

                synchronized (Master.jobQueue) {
                    Master.jobQueue.offer(job);
                }

                DaemonHelper.writeLog("MASTER:" + operation + " request", socket.getRemoteSocketAddress().toString());
                // to ensure that each job in the master will be done sequentially
                while (!Master.jobQueue.peek().equals(job)) {
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                // applying consistent hashing to the sdfsfilename to find the target nodes
                List<String> targetNodes = Hash.getTargetNode(Hash.hashing(sdfsfilename, 8));

                System.out.println("Target nodes: ");
                for (String target : targetNodes) System.out.println(target);
                System.out.println("================================");

                List<Socket> replicaSockets = new ArrayList<>();
                List<DataOutputStream> replicaOutputStreams = new ArrayList<>();
                List<DataInputStream> replicaInputStreams = new ArrayList<>();
                for (int i = 0; i < targetNodes.size(); i++) {
                    Socket replicaSocket = new Socket(
                            targetNodes.get(i).split("#")[1], Daemon.filePortNumber);
                    replicaSockets.add(replicaSocket);
                    replicaOutputStreams.add(new DataOutputStream(replicaSocket.getOutputStream()));
                    replicaInputStreams.add(new DataInputStream(replicaSocket.getInputStream()));
                }

                switch (operation) {
                    case "PUT": {

                        for (DataOutputStream out : replicaOutputStreams) {
                            out.writeUTF("PUT REPLICA");
                            out.writeUTF(sdfsfilename);
                        }
                        long fileTimeStamp = 0;
                        if (Master.fileList.containsKey(sdfsfilename)) {
                            long[] fileInfo = Master.fileList.get(sdfsfilename);
                            fileTimeStamp = fileInfo[0];
                            long latestUpdateTime = fileInfo[1];
                            if (System.currentTimeMillis() - latestUpdateTime <= 60000) {
                                clientOut.writeUTF("CONFIRM");
                                // wait for user's confirmation
                                String clientResponse = clientData.readUTF();
                                // if client replies "N", abort the update
                                // and send the information to targetNodes
                                if (clientResponse.equals("N")) {
                                    for (DataOutputStream out : replicaOutputStreams)
                                        out.writeUTF("ABORT");
                                    System.out.println("MASTER: Update aborted!");
                                    clientOut.writeUTF("ABORT");
                                    break;
                                } else {
                                    fileTimeStamp++;
                                }
                            } else {
                                clientOut.writeUTF("ACCEPT");
                                fileTimeStamp++;
                            }

                        } else {
                            clientOut.writeUTF("ACCEPT");
                        }
                        for (DataOutputStream out : replicaOutputStreams) {
                            out.writeUTF("KEEP");
                            out.writeLong(fileTimeStamp);
                        }
                        // start to receive the data and put it in the appropriate nodes
                        MasterThreadHelper.transferFile(clientData, replicaOutputStreams);
                        // implement quorum write, once writing of quorum of replicas are done
                        // reply writing finished to user
                        int count = 0;
                        while (count < Math.min(NUM_OF_REPLICA, targetNodes.size()) / 2 + 1) {
                            for (DataInputStream in : replicaInputStreams) {
                                if (in.available() != 0) {
                                    if (in.readUTF().equals("RECEIVED"))
                                        count++;
                                }
                            }
                        }
                        Master.fileList.put(
                                sdfsfilename, new long[]{fileTimeStamp, System.currentTimeMillis()});
                        Master.fileReplica.put(
                                sdfsfilename, targetNodes);
                        // MasterThreadHelper.masterSync();
                        clientOut.writeUTF("DONE");
                        break;
                    }
                    case "GET": {

                        for (DataOutputStream out : replicaOutputStreams) {
                            out.writeUTF("GET REPLICA");
                            out.writeUTF(sdfsfilename);
                        }

                        int count = 0;
                        List<Long> fileTimeStamps = new ArrayList<>();
                        Map<Long, Integer> map = new HashMap<>();
                        while (count < Math.min(NUM_OF_REPLICA, targetNodes.size()) / 2 + 1) {
                            for (int i = 0; i < replicaInputStreams.size(); i++) {
                                DataInputStream in = replicaInputStreams.get(i);
                                if (in.available() != 0) {
                                    long timeStamp = in.readLong();
                                    fileTimeStamps.add(timeStamp);
                                    map.put(timeStamp, i);
                                    count++;
                                }
                            }
                        }
                        Collections.sort(fileTimeStamps);
                        long latestTimeStamp = fileTimeStamps.get(fileTimeStamps.size() - 1);

                        if (latestTimeStamp == -1) {
                            clientOut.writeUTF("FILE_NOT_FOUND");
                            System.out.println("MASTER: file not found!");
                        } else {
                            clientOut.writeUTF("FILE_FOUND");
                            int newestNode = map.get(latestTimeStamp);
                            for (int i = 0; i < replicaOutputStreams.size(); i++) {
                                DataOutputStream out = replicaOutputStreams.get(i);
                                if (i == newestNode) {
                                    out.writeUTF("RESUME");
                                } else {
                                    out.writeUTF("HALT");
                                }
                            }
                            // start to receive the data, and transfer it to the client
                            DataInputStream source = replicaInputStreams.get(newestNode);
                            List<DataOutputStream> sink = new ArrayList<>();
                            sink.add(clientOut);
                            MasterThreadHelper.transferFile(source, sink);
                        }
                        break;
                    }

                    case "DELETE": {

                        MasterThreadHelper.deleteFile(
                                sdfsfilename, replicaOutputStreams, replicaInputStreams);
                        Master.fileList.remove(sdfsfilename);
                        Master.fileReplica.remove(sdfsfilename);
                        // MasterThreadHelper.masterSync();
                        clientOut.writeUTF("DONE");
                        break;
                    }

                    case "LIST": {

                        for (DataOutputStream out : replicaOutputStreams) {
                            out.writeUTF("LIST REPLICA");
                            out.writeUTF(sdfsfilename);
                        }

                        int count = 0;
                        List<String> fileHolders = new ArrayList<>();
                        // deal with the case that
                        while (count < Math.min(NUM_OF_REPLICA, targetNodes.size())) {
                            for (int i = 0; i < replicaInputStreams.size(); i++) {
                                DataInputStream in = replicaInputStreams.get(i);
                                if (in.available() != 0) {
                                    String response = in.readUTF();
                                    if (response.equals("FILE_FOUND")) {
                                        fileHolders.add(targetNodes.get(i).split("#")[1]);
                                    }
                                    count++;
                                }
                            }
                        }

                        if (fileHolders.size() > 0) {
                            String queryResult = String.join("#",
                                    fileHolders.toArray(new String[fileHolders.size()]));
                            clientOut.writeUTF(queryResult);
                        } else clientOut.writeUTF("EMPTY");
                        break;
                    }

                    /****************************************
                     * Below cases are for graph processing
                     ****************************************/

                    case "SAVA": {
                        long tic = System.currentTimeMillis();
                        for (DataOutputStream out : replicaOutputStreams) {
                            out.writeUTF("PUT REPLICA");
                            out.writeUTF(sdfsfilename);
                        }
                        // taskInfo should be in the form:
                        // [client ID, task type (pagerank/sssp), params for the corresponding task]
                        int numOfExtraInfo = clientData.readInt();
                        for (int i = 0; i < numOfExtraInfo; i++) {
                            Master.taskInfo.add(clientData.readUTF());
                        }
                        long fileTimeStamp = 0;
                        if (Master.fileList.containsKey(sdfsfilename)) {
                            long[] fileInfo = Master.fileList.get(sdfsfilename);
                            fileTimeStamp = fileInfo[0];
                            long latestUpdateTime = fileInfo[1];
                            if (System.currentTimeMillis() - latestUpdateTime <= 60000) {
                                clientOut.writeUTF("CONFIRM");
                                // wait for user's confirmation
                                String clientResponse = clientData.readUTF();
                                // if client replies "N", abort the update
                                // and send the information to targetNodes
                                if (clientResponse.equals("N")) {
                                    for (DataOutputStream out : replicaOutputStreams)
                                        out.writeUTF("ABORT");
                                    System.out.println("MASTER: Update aborted!");
                                    clientOut.writeUTF("ABORT");
                                    break;
                                } else {
                                    fileTimeStamp++;
                                }
                            } else {
                                clientOut.writeUTF("ACCEPT");
                                fileTimeStamp++;
                            }

                        } else {
                            clientOut.writeUTF("ACCEPT");
                        }
                        for (DataOutputStream out : replicaOutputStreams) {
                            out.writeUTF("KEEP");
                            out.writeLong(fileTimeStamp);
                        }
                        // start to receive the data and put it in the appropriate nodes
                        // Also store the parsed graph in Master's memory
                        // This is the only difference between "put" and "sava put"
                        long fileSize = clientData.readLong();
                        for (DataOutputStream dataSink: replicaOutputStreams)
                            dataSink.writeLong(fileSize);

                        byte[] buffer = new byte[Daemon.bufferSize];
                        byte[] remainedBytes = {};
                        int bytes;
                        Map<Integer, Vertex> graph = new HashMap<>();

                        while (fileSize > 0 &&
                                (bytes = clientData.read(buffer, 0,
                                        (int) Math.min(Daemon.bufferSize, fileSize))) != -1) {
                            // send the file to replicas
                            for (DataOutputStream dataSink: replicaOutputStreams)
                                dataSink.write(buffer, 0, bytes);

                            // parse the graph
                            byte[] combinedBytes = new byte[remainedBytes.length + Daemon.bufferSize];
                            System.arraycopy(
                                    remainedBytes, 0, combinedBytes, 0, remainedBytes.length);
                            System.arraycopy(
                                    buffer, 0, combinedBytes,
                                    remainedBytes.length, (int) Math.min(Daemon.bufferSize, fileSize));
                            List<Integer> endOfLineIdx = new ArrayList<>();
                            for (int i = 0; i < combinedBytes.length; i++) {
                                if (combinedBytes[i] == (byte)'\n')
                                    endOfLineIdx.add(i);
                            }
                            int prevIdx = 0;
                            for (int idx: endOfLineIdx) {
                                String line = new String(Arrays.copyOfRange(combinedBytes, prevIdx, idx));
                                line = line.trim();
                                if (line.length() > 0 && line.charAt(0) != '#') {
                                    String[] parsedLine = line.split("\\s+");
                                    int src = Integer.parseInt(parsedLine[0]);
                                    int dest = Integer.parseInt(parsedLine[1]);
                                    Vertex v;
                                    // if src is a new vertex, create a new instance of Vertex
                                    if (!graph.containsKey(src))
                                        v = new Vertex(src, Double.MAX_VALUE);
                                    else v = graph.get(src);
                                    v.addNeighbor(dest);
                                    graph.put(src, v);
                                    if (!graph.containsKey(dest)) {
                                        v = new Vertex(dest, Double.MAX_VALUE);
                                        graph.put(dest, v);
                                    }
                                }
                                prevIdx = idx;
                            }
                            remainedBytes = Arrays.copyOfRange(combinedBytes, prevIdx, combinedBytes.length);
                            fileSize -= bytes;
                        }

                        // implement quorum write, once writing of quorum of replicas are done
                        // reply writing finished to user
                        int count = 0;
                        while (count < Math.min(NUM_OF_REPLICA, targetNodes.size()) / 2 + 1) {
                            for (DataInputStream in : replicaInputStreams) {
                                if (in.available() != 0) {
                                    if (in.readUTF().equals("RECEIVED"))
                                        count++;
                                }
                            }
                        }

                        Master.graph = graph;
                        // graph transmission completed

                        Master.fileList.put(sdfsfilename,
                                new long[]{fileTimeStamp, System.currentTimeMillis()});
                        Master.fileReplica.put(sdfsfilename,
                                targetNodes);

                        // synchronize the received graph between master and backup masters
                        MasterThreadHelper.masterSynchronization();

                        // reply to the client
                        clientOut.writeUTF("DONE");

                        long toc = System.currentTimeMillis();
                        System.out.println(
                                "Processing time for graph parsing: " + (toc - tic) / 1000. + "(sec)");


                        /******************************
                         ****** GRAPH COMPUTING *******
                         ******************************/

                        tic = System.currentTimeMillis();

                        MasterThreadHelper.graphComputing();

                        toc = System.currentTimeMillis();
                        System.out.println(
                                "Processing time for graph computing: " + (toc - tic) / 1000. + "(sec)");
                        clientOut.writeUTF("DONE");
                        break;

                    }
                    case "SAVA RETRIGGER": {

                        System.out.println("SAVA RETRIGGER");
                        List<Socket> workerSkts = new ArrayList<>();
                        List<ObjectOutputStream> workerOuts = new ArrayList<>();
                        List<ObjectInputStream> workerIns = new ArrayList<>();

                        // update Master.workers
                        // since the assumption is that once master fails, the worker list will be unchanged
                        // we can pick any worker to sync the status of the graph processing task

                        MasterThreadHelper.checkWorker(Master.taskInfo.get(0));

                        String[] workers = Master.workers.split("_");
                        String worker = workers[0];
                        int status;
                        try {
                            Socket skt = new Socket(worker.split("#")[1], Daemon.graphPortNumber);
                            ObjectOutputStream dos = new ObjectOutputStream(skt.getOutputStream());
                            ObjectInputStream dis = new ObjectInputStream(skt.getInputStream());
                            dos.writeUTF("NEW_MASTER");
                            dos.flush();

                            // sync the status of the graph computing task
                            // status 0: partition is not yet done
                            // status 1: partition is done
                            status = dis.readInt();

                            switch (status) {
                                case 0:
                                    Master.workers = "";
                                    break;
                                case 1:
                                    Master.iteration = dis.readInt() + 1;
                                    break;
                            }

                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                        // Synchronization done
                        // resume the graph computing task

                        MasterThreadHelper.graphComputing();

                        // inform the client that the task is done
                        clientOut.writeUTF("DONE");
                    }
                }
                System.out.println(Master.jobQueue.peek() + " completed!");
                // release the resources occupied by this thread
                Master.jobQueue.poll();
                for (Socket skt : replicaSockets) {
                    skt.close();
                }
                DaemonHelper.writeLog(
                        "MASTER:" + operation + " done",
                        socket.getRemoteSocketAddress().toString());
            }

        } catch (Exception e) {
            Master.jobQueue.poll();
            e.printStackTrace();
        }

    }
}
