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

            if (operation.equals("SYNC_PULL")) {

                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                oos.writeObject(Master.fileList);
                oos.writeObject(Master.fileReplica);
                oos.writeObject(Master.taskInfo);
                oos.writeInt(Master.iteration);
                oos.flush();
                oos.writeUTF(Master.workers);
                oos.flush();
                oos.writeObject(Master.graph);
                oos.writeObject(Master.partition);

            } else if (operation.equals("SYNC_PUSH")) {

                int size = clientData.readInt();
                Map<String, long[]> tempMap = new HashMap<>();
                for (int i = 0; i < size; i++) {
                    String kvPair = clientData.readUTF();
                    String key = kvPair.split("#")[0];
                    String val = kvPair.split("#")[1];
                    val = val.substring(1, val.length() - 1).replace(" ", "");
                    String[] valParts = val.split(",");
                    long[] value = new long[valParts.length];
                    for (int j = 0; j < valParts.length; j++)
                        value[j] = Long.parseLong(valParts[j]);
                    tempMap.put(key, value);
                }
                Master.fileList = tempMap;
                Map<String, List<String>> replicaMap  = new HashMap<>();
                size = clientData.readInt();
                for (int i = 0; i < size; i++) {
                    String kvPair = clientData.readUTF();
                    String key = kvPair.split("_")[0];
                    String val = kvPair.split("_")[1];
                    val = val.substring(1, val.length() - 1).replace(" ", "");
                    String[] valParts = val.split(",");
                    List<String> value = Arrays.asList(valParts);
                    replicaMap.put(key, value);
                }
                Master.fileReplica = replicaMap;

            } else if (operation.equals("SAVA SYNC")) {
                // for backup master to sync meta data of graph processing task
                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                Master.taskInfo = (List<String>) ois.readObject();
                Master.graph = (HashMap<Integer, Vertex>) ois.readObject();
                oos.writeUTF("DONE");
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
                        System.out.println("Graph");
                        // synchronize the received graph between master and backup masters
                        List<String> backupMasters = MasterThreadHelper.getBackupMasters();

                        List<Socket> backupSockets = new ArrayList<>();
                        List<ObjectOutputStream> backupOuts = new ArrayList<>();
                        List<ObjectInputStream> backupIns = new ArrayList<>();
                        try {
                            for (String backupMaster : backupMasters) {
                                Socket skt = new Socket(backupMaster.split("#")[1], Daemon.masterPortNumber);
                                ObjectOutputStream oos = new ObjectOutputStream(skt.getOutputStream());
                                ObjectInputStream ois = new ObjectInputStream(skt.getInputStream());
                                backupSockets.add(skt);
                                backupOuts.add(oos);
                                backupIns.add(ois);
                            }

                            for (int i = 0; i < backupMasters.size(); i++) {
                                backupOuts.get(i).writeUTF("SAVA SYNC");
                                backupOuts.get(i).writeObject(Master.taskInfo);
                                backupOuts.get(i).writeObject(Master.graph);
                            }

                            for (ObjectInputStream ois : backupIns) ois.readUTF();
                        } catch (Exception e) {
                            // e captures the case that at least one backup master failed
                            // based on the assumption, current master will not fail
                            // continue our graph processing task
                        }
                        // graph synchronization completed

                        // reply to the client
                        clientOut.writeUTF("DONE");

                        Master.fileList.put(sdfsfilename,
                                new long[]{fileTimeStamp, System.currentTimeMillis()});
                        Master.fileReplica.put(sdfsfilename,
                                targetNodes);

                        List<Socket> workerSkts = new ArrayList<>();
                        List<ObjectOutputStream> workerOuts = new ArrayList<>();
                        List<ObjectInputStream> workerIns = new ArrayList<>();
                        Map<String, Integer> map = new HashMap<>();
                        Map<Integer, String> reversedMap = new HashMap<>();

                        boolean isIteration = false;
                        int numOfIteration = -1;
                        String terminateCondition = Master.taskInfo.get(3);
                        if (terminateCondition.matches("\\d+")) {
                            isIteration = true;
                            numOfIteration = Integer.parseInt(terminateCondition);
                        }
                        System.out.println(Master.graph.toString());

                        // start to do iterations for graph computing
                        while (true) {

                            //backupMasters = MasterThreadHelper.getBackupMasters();
                            /*****************************
                             ****** PARTITION PHASE ******
                             *****************************/
                            // check the worker status and take different actions:
                            // 0: at least one worker fails
                            // 1: at least one worker rejoins
                            // 2: worker list unchanged
                            int status = MasterThreadHelper.checkWorker(Master.taskInfo.get(0));
                            System.out.println("Status:" + status);
                            try {
                                // initialize the graph and do partition
                                switch (status) {
                                    case 0: {
                                        // some workers fail, restart the task
                                        Master.iteration = 1;
                                        workerSkts.clear();
                                        workerOuts.clear();
                                        workerIns.clear();
                                        map.clear();
                                        reversedMap.clear();

                                        String[] workers = Master.workers.split("_");
                                        for (int i = 0; i < workers.length; i++) {
                                            String worker = workers[i];
                                            //System.out.println(worker);
                                            Socket skt = new Socket(worker.split("#")[1], Daemon.graphPortNumber);
                                            workerSkts.add(skt);
                                            workerOuts.add(new ObjectOutputStream(skt.getOutputStream()));
                                            workerIns.add(new ObjectInputStream(skt.getInputStream()));
                                            map.put(worker, i);
                                            reversedMap.put(i, worker);
                                        }
                                        for (ObjectOutputStream out: workerOuts) {
                                            out.writeUTF(Master.taskInfo.get(1).toUpperCase());
                                            out.flush();
                                            out.writeInt((isIteration? 0: 1) +
                                                    (Master.taskInfo.get(1).equals("pagerank")? 1: 0));
                                            // send the damping factor to workers
                                            if(Master.taskInfo.get(1).equals("pagerank")) {
                                                out.writeDouble(Double.parseDouble(Master.taskInfo.get(2)));
                                                out.flush();
                                            }
                                            if(!isIteration) {
                                                out.writeDouble(Double.parseDouble(terminateCondition));
                                                out.flush();
                                            }
                                        }
                                        for (ObjectInputStream in: workerIns) {
                                            in.readUTF();
                                        }
                                        MasterThreadHelper.graphPartition(workerIns, workerOuts, map, reversedMap);
                                        break;
                                    }
                                    case 1: {
                                        // some workers rejoin, re-partition the task
                                        // and create new socket to new workers
                                        String[] workers = Master.workers.split("_");
                                        for (int i = 0; i < workers.length; i++) {
                                            String worker = workers[i];
                                            if (!map.containsKey(worker)) {
                                                Socket skt = new Socket(
                                                        worker.split("#")[1], Daemon.graphPortNumber);
                                                workerSkts.add(skt);
                                                workerOuts.add(new ObjectOutputStream(skt.getOutputStream()));
                                                workerIns.add(new ObjectInputStream(skt.getInputStream()));
                                                map.put(worker, map.size());
                                                reversedMap.put(reversedMap.size(), worker);
                                            }
                                        }
                                        MasterThreadHelper.graphPartition(workerIns, workerOuts, map, reversedMap);
                                        break;
                                    }
                                    case 2:
                                        // worker list unchanged, do nothing
                                        break;
                                }
                                System.out.println("ITERATION " + Master.iteration);
                                // partition done, start this iteration
                                for (ObjectOutputStream out: workerOuts) {
                                    out.writeUTF("ITERATION");
                                    out.flush();
                                }

                                int haltCount = 0;
                                for (int i = 0; i < workerIns.size(); i++) {
                                    String res = workerIns.get(i).readUTF();
                                    System.out.println(reversedMap.get(i) + ": " + res);
                                    if (res.equals("HATL"))
                                        haltCount++;
                                }
                                /*
                                for (ObjectInputStream in: workerIns) {
                                    if (in.readUTF().equals("HALT"))
                                        haltCount ++;
                                }*/

                                Master.iteration ++;
                                // if all workers vote to halt or reaches the iteration upper limit
                                // terminate the task and store the results in the SDFS
                                if ((!isIteration && haltCount == workerIns.size())
                                        || (isIteration && Master.iteration == numOfIteration)) {

                                    for (ObjectOutputStream out: workerOuts) {
                                        out.writeUTF("TERMINATE");
                                        out.flush();
                                    }

                                    List<String> results = new ArrayList<>();

                                    for (ObjectInputStream in: workerIns) {
                                        int size = in.readInt();
                                        for (int i = 0; i < size; i++) {
                                            int vertexID = in.readInt();
                                            double vertexValue = in.readDouble();
                                            Formatter f = new Formatter();
                                            results.add(vertexID + "," + f.format("%.5f", vertexValue));
                                        }
                                    }
                                    Collections.sort(results);
                                    for (String s: results)
                                        System.out.println(s);
                                    /*
                                    // save the file into the SDFS
                                    String outputFileName = Master.taskInfo.get(4);
                                    List<String> outTgtNodes =
                                            Hash.getTargetNode(Hash.hashing(outputFileName, 8));

                                    List<Socket> outSkts = new ArrayList<>();
                                    List<DataInputStream> outSktIns = new ArrayList<>();
                                    List<DataOutputStream> outSktOuts = new ArrayList<>();

                                    for (String outTgtNode: outTgtNodes) {
                                        Socket skt = new Socket(
                                                outTgtNode.split("#")[1], Daemon.filePortNumber);
                                        outSkts.add(skt);
                                        outSktIns.add(new DataInputStream(skt.getInputStream()));
                                        outSktOuts.add(new DataOutputStream(skt.getOutputStream()));
                                    }

                                    for (DataOutputStream out: outSktOuts) {
                                        out.writeUTF("PUT REPLICA");
                                        out.writeUTF(outputFileName);
                                        out.writeUTF("KEEP");
                                    }*/
                                    // leave the while loop
                                    break;
                                }

                            } catch (Exception e) {
                                // e captures the case that during partition, some workers fails
                                // since we will check worker status at the beginning of each iteration
                                // we don't need to do any exception handling
                                System.out.println("In exception");
                                Master.workers = "";
                                try {
                                    Thread.sleep(1000);
                                } catch (InterruptedException ie) {
                                    // do nothing
                                }
                            }
                        }
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
