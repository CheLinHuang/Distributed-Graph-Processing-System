import javax.print.ServiceUI;
import java.io.*;
import java.util.*;
import java.net.*;
import java.nio.*;

public class MasterThreadHelper {

    public static void transferFile(
            DataInputStream dataSource,
            List<DataOutputStream> dataSinks) throws Exception {
        long fileSize = dataSource.readLong();
        for (DataOutputStream dataSink: dataSinks)
            dataSink.writeLong(fileSize);

        byte[] buffer = new byte[Daemon.bufferSize];
        int bytes;
        while (fileSize > 0 &&
                (bytes = dataSource.read(buffer, 0,
                        (int) Math.min(Daemon.bufferSize, fileSize))) != -1) {
            for (DataOutputStream dataSink: dataSinks)
                dataSink.write(buffer, 0, bytes);
            fileSize -= bytes;
        }
    }

    public static void getFile(
            String sdfsfilename,
            List<DataOutputStream> clientOutputStreams,
            List<DataOutputStream> replicaOutputStreams,
            List<DataInputStream> replicaInputStreams) throws Exception {

        for (DataOutputStream out : replicaOutputStreams) {
            out.writeUTF("GET REPLICA");
            out.writeUTF(sdfsfilename);
        }

        int count = 0;
        int NUM_OF_REPLICA = replicaOutputStreams.size();
        List<Long> fileTimeStamps = new ArrayList<>();
        Map<Long, Integer> map = new HashMap<>();
        while (count < NUM_OF_REPLICA / 2 + 1) {
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
            for (DataOutputStream clientOut: clientOutputStreams)
                clientOut.writeUTF("FILE_NOT_FOUND");
        } else {
            for (DataOutputStream clientOut: clientOutputStreams)
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
            long fileSize = source.readLong();
            for (DataOutputStream clientOut: clientOutputStreams)
                clientOut.writeLong(fileSize);

            byte[] buffer = new byte[Daemon.bufferSize];
            int bytes;
            while (fileSize > 0 &&
                    (bytes = source.read(buffer, 0,
                            (int) Math.min(Daemon.bufferSize, fileSize))) != -1) {
                for (DataOutputStream clientOut: clientOutputStreams)
                    clientOut.write(buffer, 0, bytes);
                fileSize -= bytes;
            }
            // replicaOutputStreams.get(newestNode).writeUTF("DONE");
        }
    }

    public static void deleteFile(
            String sdfsfilename,
            List<DataOutputStream> replicaOutputStreams,
            List<DataInputStream> replicaInputStreams) throws Exception {

        for (DataOutputStream out : replicaOutputStreams) {
            out.writeUTF("DELETE REPLICA");
            out.writeUTF(sdfsfilename);
        }
        int count = 0;
        while (count < replicaOutputStreams.size()) {
            for (DataInputStream in : replicaInputStreams) {
                if (in.available() != 0) {
                    String response = in.readUTF();
                    if (response.equals("DONE"))
                        count++;
                }
            }
        }
    }

    public static void masterSynchronization() {

        // get the list of backup masters
        List<String> backupMasters = new ArrayList<>();
        synchronized (Daemon.masterList) {
            for (String nodeID: Daemon.masterList.keySet()) {
                if (!nodeID.equals(Daemon.ID))
                    backupMasters.add(nodeID);
            }
        }
        System.out.println(
                "Perform synchronization with: " + backupMasters.toString());
        long tic = System.currentTimeMillis();
        List<Socket> backupSockets = new ArrayList<>();
        List<DataOutputStream> backupOuts = new ArrayList<>();
        List<DataInputStream> backupIns = new ArrayList<>();
        try {
            for (String backupMaster : backupMasters) {
                Socket skt = new Socket(backupMaster.split("#")[1], Daemon.masterPortNumber);
                backupSockets.add(skt);
                backupOuts.add(new DataOutputStream(skt.getOutputStream()));
                backupIns.add(new DataInputStream(skt.getInputStream()));
            }
            for (DataOutputStream out: backupOuts) {
                out.writeUTF("SYNC");
                out.flush();

                byte[] buffer = new byte[Daemon.bufferSize];

                // sync fileList
                int size = Serialization.byteCount((HashMap<String, long[]>) Master.fileList);
                ByteBuffer bb = Serialization.serialize(
                        (HashMap<String, long[]>) Master.fileList, size);
                out.writeInt(size);
                bb.clear();
                while (size > 0) {

                    int writeSize = Math.min(Daemon.bufferSize, size);
                    bb.get(buffer, 0, writeSize);
                    out.write(buffer, 0, writeSize);
                    out.flush();
                    size -= Daemon.bufferSize;
                }

                // sync fileReplica
                size = Serialization.byteCount(Master.fileReplica);
                bb = Serialization.serialize(Master.fileReplica, size);
                System.out.println(size);
                out.writeInt(size);
                bb.clear();
                while (size > 0) {
                    int writeSize = Math.min(Daemon.bufferSize, size);
                    bb.get(buffer, 0, writeSize);
                    out.write(buffer, 0, writeSize);
                    out.flush();
                    size -= Daemon.bufferSize;
                }

                // sync taskInfo
                size = Serialization.byteCount(Master.taskInfo);
                bb = Serialization.serialize(Master.taskInfo, size);
                out.writeInt(size);
                bb.clear();
                while (size > 0) {
                    int writeSize = Math.min(Daemon.bufferSize, size);
                    bb.get(buffer, 0, writeSize);
                    out.write(buffer, 0, writeSize);
                    out.flush();
                    size -= Daemon.bufferSize;
                }

                // sync graph
                size = Serialization.byteCountGraph(Master.graph);
                bb = Serialization.serializeGraph(Master.graph, size);
                out.writeInt(size);
                bb.clear();
                while (size > 0) {
                    int writeSize = Math.min(Daemon.bufferSize, size);
                    bb.get(buffer, 0, writeSize);
                    out.write(buffer, 0, writeSize);
                    out.flush();
                    size -= Daemon.bufferSize;
                }
            }

            for (DataInputStream in : backupIns) in.readUTF();

        } catch (Exception e) {
            e.printStackTrace();
            // e captures the case that at least one backup master failed
            // based on the assumption, current master will not fail
            // continue our graph processing task
        }
        long toc = System.currentTimeMillis();
        System.out.println(
                "Processing time for synchronization: " + (toc - tic) / 1000. + " (sec)");
    }


    /****************************************
     * Below methods are for graph processing
     ****************************************/

    public static void saveResults(
            List<String> results, String sdfsFileName) throws Exception {

        long tic = System.currentTimeMillis();

        List<String> outTgtNodes =
                Hash.getTargetNode(Hash.hashing(sdfsFileName, 8));

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

        // for debugging
        int byteCount = 0;
        List<byte[]> resultBytes = new ArrayList<>();
        for (String s: results) {
            byte[] temp = (s + "\n").getBytes();
            resultBytes.add(temp);
            byteCount += temp.length;
        }

        for (DataOutputStream out: outSktOuts) {
            out.writeUTF("PUT REPLICA");
            out.writeUTF(sdfsFileName);
            out.writeUTF("KEEP");
            out.writeLong(1);
            out.writeLong(byteCount);
        }

        byte[] remainedBytes = {};
        for (byte[] bytes: resultBytes) {
            byte[] combinedBytes = new byte[remainedBytes.length + bytes.length];
            System.arraycopy(
                    remainedBytes, 0, combinedBytes, 0, remainedBytes.length);
            System.arraycopy(
                    bytes, 0, combinedBytes, remainedBytes.length, bytes.length);

            if (combinedBytes.length >= Daemon.bufferSize) {
                for (DataOutputStream out: outSktOuts)
                    out.write(combinedBytes, 0, Daemon.bufferSize);
                remainedBytes = Arrays.copyOfRange(combinedBytes, Daemon.bufferSize, combinedBytes.length);

            } else remainedBytes = combinedBytes;
        }
        for (DataOutputStream out: outSktOuts)
            out.write(remainedBytes, 0, remainedBytes.length);

        for (DataInputStream in: outSktIns)
            in.readUTF();
    }

    public static int checkWorker(String clientID) {
        /*
        this member will check the worker list and decide
        if it need to do restart the job
        return:
            0 stands for restarting the job (worker failed)
            1 stands for repartitioning (worker rejoin)
            2 stands for do nothing
         */

        boolean initialized = true;
        if (Master.workers.length() == 0)
            initialized = false;

        boolean newWorkerAdded = false;
        boolean oldWorkerFailed = false;

        synchronized (Daemon.hashValues) {
            synchronized (Daemon.masterList) {
                int size = Daemon.hashValues.size();
                Integer[] keySet = new Integer[size];
                Daemon.hashValues.navigableKeySet().toArray(keySet);
                String workers = "";
                for (Integer key : keySet) {
                    String nodeID = Daemon.hashValues.get(key);
                    if (!nodeID.equals(clientID) && !Daemon.masterList.containsKey(nodeID)) {
                        workers += (nodeID + "_");
                        if (Master.workers.indexOf(nodeID) == -1)
                            newWorkerAdded = true;
                    }
                }
                for (String oldWorker : Master.workers.split("_")) {
                    if (workers.indexOf(oldWorker) == -1)
                        oldWorkerFailed = true;
                }
                Master.workers = workers;
            }
        }
        return (oldWorkerFailed || !initialized)? 0: (newWorkerAdded? 1: 2);
    }

    public static void pauseForTrace (long pauseTime) {

        try{
            Thread.sleep(pauseTime);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public static void graphPartition(
            List<ObjectInputStream> workerIns,
            List<ObjectOutputStream> workerOuts,
            Map<String, Integer> map,
            Map<Integer, String> reversedMap,
            Map<Integer, Double> vertexValues) throws Exception {

        int numOfWorkers = workerIns.size();
        System.out.println("# of workers: " + workerIns.size());

        String task = Master.taskInfo.get(1);

        List<HashMap<Integer, Vertex>> partition = new ArrayList<>();
        Map<String, List<String>> vertexInfo = new HashMap<>();

        for (int i = 0; i < numOfWorkers; i++)
            partition.add(new HashMap<>());

        switch (task) {
            case "pagerank":
                for (int key: Master.graph.keySet()) {
                    int hashValue = String.valueOf(key).hashCode() % numOfWorkers;
                    List<String> list;
                    if (!vertexInfo.containsKey(String.valueOf(key)))
                        list = new ArrayList<>();
                    else list = vertexInfo.get(String.valueOf(key));
                    list.add(reversedMap.get(hashValue));
                    vertexInfo.put(String.valueOf(key), list);

                    Vertex v = Master.graph.get(key);
                    if (vertexValues.size() == 0)
                        v.setValue(1);
                    else
                        v.setValue(vertexValues.get(key));
                    partition.get(hashValue).put(key, v);
                }
                break;
            case "sssp":
                int sourceNode = Integer.parseInt(Master.taskInfo.get(2));
                for (int key: Master.graph.keySet()) {
                    int hashValue = String.valueOf(key).hashCode() % numOfWorkers;
                    List<String> list;
                    if (!vertexInfo.containsKey(String.valueOf(key)))
                        list = new ArrayList<>();
                    else list = vertexInfo.get(String.valueOf(key));
                    list.add(reversedMap.get(hashValue));
                    vertexInfo.put(String.valueOf(key), list);

                    Vertex v = Master.graph.get(key);
                    if (vertexValues.size() == 0) {
                        if (key == sourceNode) v.setValue(0);
                        else v.setValue(Double.MAX_VALUE);
                    } else
                        v.setValue(vertexValues.get(key));
                    partition.get(hashValue).put(key, v);
                }
                break;
        }

        for (int i = 0; i < numOfWorkers; i++) {
            ObjectOutputStream out = workerOuts.get(i);
            byte[] buffer = new byte[Daemon.bufferSize];
            // send the sub-graph
            int size = Serialization.byteCountGraph(partition.get(i));
            ByteBuffer bb = Serialization.serializeGraph(partition.get(i), size);
            out.writeUTF("ADD");
            out.flush();
            out.writeInt(size);
            out.flush();
            bb.clear();
            while (size > 0) {
                int writeSize = Math.min(Daemon.bufferSize, size);
                bb.get(buffer, 0, writeSize);
                out.write(buffer, 0, writeSize);
                out.flush();
                size -= Daemon.bufferSize;
            }

            size = Serialization.byteCount(vertexInfo);
            bb = Serialization.serialize(vertexInfo, size);
            out.writeUTF("NEIGHBOR_INFO");
            out.flush();
            out.writeInt(size);
            out.flush();
            bb.clear();
            while (size > 0) {
                int writeSize = Math.min(Daemon.bufferSize, size);
                bb.get(buffer, 0, writeSize);
                out.write(buffer, 0, writeSize);
                out.flush();
                size -= Daemon.bufferSize;
            }
        }

        for (ObjectInputStream in: workerIns)
            in.readUTF();
    }

    public static void graphComputing(boolean retrigger) {

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
        if (retrigger) {
            String[] workers = Master.workers.split("_");
            for (int i = 0; i < workers.length; i++) {
                String worker = workers[i];
                System.out.println(worker);
                try {
                    Socket skt = new Socket(worker.split("#")[1], Daemon.graphPortNumber);
                    workerSkts.add(skt);
                    workerOuts.add(new ObjectOutputStream(skt.getOutputStream()));
                    workerIns.add(new ObjectInputStream(skt.getInputStream()));
                    map.put(worker, i);
                    reversedMap.put(i, worker);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        long iterationTime = 0;

        while (true) {

            /******************************
             ****** PARTITION PHASE *******
             ******************************/
            // check the worker status and take different actions:
            // 0: at least one worker fails
            // 1: at least one worker rejoins
            // 2: worker list unchanged
            int status = MasterThreadHelper.checkWorker(Master.taskInfo.get(0));
            System.out.println("Status:" + status);

            try {

                if (status != 2) {

                    long ptic = System.currentTimeMillis();

                    if (status == 0) {
                        Master.iteration = 1;
                    }

                    // if new worker added, gather the calculation results from
                    // all the workers and re-distribute it
                    Map<Integer, Double> vertexValues = new HashMap<>();
                    if (status == 1) {

                        for (ObjectOutputStream out: workerOuts) {
                            out.writeUTF("TERMINATE");
                            out.flush();
                        }
                        for (ObjectInputStream in: workerIns) {
                            int size = in.readInt();
                            for (int i = 0; i < size; i++) {
                                int vertexID = in.readInt();
                                double vertexValue = in.readDouble();
                                vertexValues.put(vertexID, vertexValue);
                            }
                        }
                    }

                    workerSkts.clear();
                    workerOuts.clear();
                    workerIns.clear();
                    map.clear();
                    reversedMap.clear();

                    String[] workers = Master.workers.split("_");
                    for (int i = 0; i < workers.length; i++) {
                        String worker = workers[i];
                        System.out.println(worker);
                        Socket skt = new Socket(worker.split("#")[1], Daemon.graphPortNumber);
                        workerSkts.add(skt);
                        workerOuts.add(new ObjectOutputStream(skt.getOutputStream()));
                        workerIns.add(new ObjectInputStream(skt.getInputStream()));
                        map.put(worker, i);
                        reversedMap.put(i, worker);
                    }
                    // initialize workers
                    for (ObjectOutputStream out: workerOuts) {
                        out.writeUTF(Master.taskInfo.get(1).toUpperCase());
                        out.flush();
                        out.writeInt((isIteration? 0: 1) +
                                (Master.taskInfo.get(1).equals("pagerank")? 1: 0));
                        out.flush();
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
                    // make sure that all the workers have received task-specific information
                    for (ObjectInputStream in: workerIns) {
                        in.readUTF();
                    }
                    // start to distribute the graph into each worker
                    MasterThreadHelper.graphPartition(
                            workerIns, workerOuts, map, reversedMap, vertexValues);

                    long ptoc = System.currentTimeMillis();
                    System.out.println(
                            "Processing time for graph partitioning: "
                                    + (ptoc - ptic) / 1000. + " (sec)");
                }

                long tic = System.currentTimeMillis();
                // partition done, start this iteration
                for (ObjectOutputStream out: workerOuts) {
                    out.writeUTF("ITERATION");
                    out.flush();
                }
                System.out.println("ITERATION " + Master.iteration);

                int haltCount = 0;
                boolean workerFailed = false;
                for (ObjectInputStream in: workerIns) {
                    try {
                        String response = in.readUTF();
                        if (response.equals("HALT"))
                            haltCount ++;
                    } catch (Exception e) {
                        workerFailed = true;
                    }
                }

                if (workerFailed) {
                    System.out.println("At least one worker fails, restart the task");
                    Master.workers = "";
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException ie) {
                        // do nothing
                    }
                    continue;
                }
                System.out.println("ITERATION " + Master.iteration + " DONE");
                long toc = System.currentTimeMillis();
                iterationTime += (toc - tic);

                // if all workers vote to halt or reaches the iteration upper limit
                // terminate the task and store the results in the SDFS
                if ((!isIteration && haltCount == workerIns.size())
                        || (isIteration && Master.iteration == numOfIteration)) {

                    System.out.println("Iteration time: " + iterationTime / 1000. + " (sec)");

                    long dtic = System.currentTimeMillis();
                    for (ObjectOutputStream out: workerOuts) {
                        out.writeUTF("TERMINATE");
                        out.flush();
                    }


                    List<List<String>> subGraphs = new ArrayList<>();
                    List<Boolean> flags = new ArrayList<>();
                    List<Thread> threads = new ArrayList<>();

                    // List<String> results = new ArrayList<>();

                    for (ObjectInputStream in: workerIns) {
                        Thread t = new CollectResults(subGraphs, flags, in);
                        threads.add(t);
                        t.start();
                    }

                    for (Thread t: threads)
                        t.join();

                    long tiic = System.currentTimeMillis();

                    List<String> results = new ArrayList<>();

                    PriorityQueue<ListIterator<String>> pq =
                            new PriorityQueue<>(new Comparator<ListIterator<String>>() {
                        @Override
                        public int compare(ListIterator<String> o1, ListIterator<String> o2) {
                            Double do1 = Double.parseDouble(o1.next().split(",")[1]);
                            Double do2 = Double.parseDouble(o2.next().split(",")[1]);
                            o1.previous();
                            o2.previous();
                            return do2.compareTo(do1);
                        }
                    });

                    for (List<String> subGraph: subGraphs)
                        pq.offer(subGraph.listIterator());

                    while(!pq.isEmpty()) {
                        ListIterator<String> iter = pq.poll();
                        results.add(iter.next());
                        if (iter.hasNext())
                            pq.offer(iter);
                    }
                    long tooc = System.currentTimeMillis();
                    System.out.println(
                            "Processing time for merging the results: "
                                    + (tooc - tiic) / 1000. + "(sec)");

                    long dtoc = System.currentTimeMillis();
                    System.out.println(
                            "Processing time for collecting the results: "
                                    + (dtoc - dtic) / 1000. + "(sec)");
                    // save the results in the SDFS
                    MasterThreadHelper.saveResults(results, Master.taskInfo.get(4));
                    long dtoc2 = System.currentTimeMillis();
                    System.out.println(
                            "Processing time for saving the results: "
                                    + (dtoc2 - dtoc) / 1000. + "(sec)");

                    // clear the temp file for the graph task
                    Master.clearGraphTask();
                    // leave the while loop
                    break;
                }
                Master.iteration ++;

            } catch (Exception e) {
                // e captures the case that during partition, some workers fails
                // since we will check worker status at the beginning of each iteration
                // we don't need to do any exception handling
                // e.printStackTrace();
                System.out.println("In exception");
                Master.workers = "";

                try {
                    for (Socket skt: workerSkts)
                        skt.close();
                    Thread.sleep(2500);
                } catch (Exception ie) {
                    // do nothing
                }
            }
        }

    }
}
