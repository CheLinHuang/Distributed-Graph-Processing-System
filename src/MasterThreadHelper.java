import java.io.*;
import java.util.*;
import java.net.*;

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

    /****************************************
     * Below methods are for graph processing
     ****************************************/

    public static List<String> getBackupMasters() {

        List<String> res = new ArrayList<>();
        synchronized (Daemon.masterList) {
            for (String nodeID: Daemon.masterList.keySet()) {
                if (!nodeID.equals(Daemon.master))
                    res.add(nodeID);
            }
        }
        return res;
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
        if (Master.workers.length() == 0) return 0;

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
                        if (nodeID.indexOf(Master.workers) == -1)
                            newWorkerAdded = true;
                    }
                }
                // workers = workers.substring(0, workers.length() - 1);
                for (String oldWorker : Master.workers.split("_")) {
                    if (oldWorker.indexOf(workers) == -1)
                        oldWorkerFailed = true;
                }
                Master.workers = workers;
            }
        }
        return oldWorkerFailed? 0: (newWorkerAdded? 1: 2);
    }

    public static void graphPartition(
            List<ObjectInputStream> workerIns,
            List<ObjectOutputStream> workerOuts,
            Map<String, Integer> map,
            Map<Integer, String> reversedMap) throws Exception {

        int numOfWorkers = workerIns.size();
        // initialize the graph and send each vertex to its assigned worker
        if (Master.iteration == 1) {
            String task = Master.taskInfo.get(1);
            switch (task) {
                case "pagerank":
                    for (int key: Master.graph.keySet()) {
                        int hashValue = String.valueOf(key).hashCode() % numOfWorkers;
                        Master.partition.put(key, reversedMap.get(hashValue));
                        Vertex v = Master.graph.get(key);
                        v.setValue(1);
                        ObjectOutputStream out = workerOuts.get(hashValue);
                        out.writeUTF("ADD");
                        out.flush();
                        out.writeObject(v);
                    }
                    for (ObjectOutputStream out: workerOuts) {
                        out.writeUTF("NEIGHBOR_INFO");
                        out.flush();
                        out.writeObject(Master.partition);
                    }
                    break;
                case "sssp":
                    int sourceNode = Integer.parseInt(Master.taskInfo.get(2));
                    for (int key: Master.graph.keySet()) {
                        int hashValue = String.valueOf(key).hashCode() % numOfWorkers;
                        Master.partition.put(key, reversedMap.get(hashValue));
                        Vertex v = Master.graph.get(key);
                        if (key == sourceNode) v.setValue(0);
                        else v.setValue(Double.MAX_VALUE);
                        ObjectOutputStream out = workerOuts.get(hashValue);
                        out.writeUTF("ADD");
                        out.flush();
                        out.writeObject(v);
                    }
                    for (ObjectOutputStream out: workerOuts) {
                        out.writeUTF("NEIGHBOR_INFO");
                        out.flush();
                        out.writeObject(Master.partition);
                    }
                    break;
            }

            for (ObjectInputStream in: workerIns) {
                in.readUTF();
            }
        } else {

            for (int key: Master.graph.keySet()) {
                int hashValue = String.valueOf(key).hashCode() % numOfWorkers;
                String oldWorker = Master.partition.get(key);
                String newWorker = reversedMap.get(hashValue);
                Master.partition.put(key, newWorker);
                // for debugging
                System.out.println(newWorker);
                System.out.println(oldWorker);
                if (!newWorker.equals(oldWorker)) {
                    ObjectOutputStream oldWorkerOut = workerOuts.get(map.get(oldWorker));
                    ObjectInputStream oldWorkerIn = workerIns.get(map.get(oldWorker));
                    ObjectOutputStream newWorkerOut = workerOuts.get(hashValue);
                    // read the corresponding vertex object
                    // from the old worker
                    oldWorkerOut.writeUTF("DELETE");
                    oldWorkerOut.flush();
                    oldWorkerOut.writeInt(key);
                    oldWorkerOut.flush();
                    // transfer the data to the new worker
                    newWorkerOut.writeUTF("ADD");
                    newWorkerOut.flush();
                    newWorkerOut.writeObject(oldWorkerIn.readObject());
                    oldWorkerOut.writeUTF("DONE");
                    oldWorkerOut.flush();
                }
            }
            for (ObjectOutputStream out: workerOuts) {
                out.writeUTF("NEIGHBOR_INFO");
                out.flush();
                out.writeObject(Master.partition);
            }

            for (ObjectInputStream in: workerIns) {
                in.readUTF();
            }
        }
    }
}
