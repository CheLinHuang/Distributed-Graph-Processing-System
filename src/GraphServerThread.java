import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.*;

public class GraphServerThread extends Thread {

    private Socket socket;

    GraphServerThread(Socket socket) {
        this.socket = socket;
    }

    @Override
    public void run() {

        try (
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream())
        ) {

            while (true) {

                String operation = in.readUTF();
                DaemonHelper.writeLog(operation, "");

                switch (operation) {
                    case "ADD": {

                        // long time = System.currentTimeMillis();

                        // build local graph
                        int length = in.readInt();

                        Serialization.deserializeGraph(getBuffer(in, length), GraphServer.graph);

                        for (int i : GraphServer.graph.keySet()) {
                            GraphServer.incoming.put(i, new ArrayList<>());
                        }

                        // System.out.println("add time " + (System.currentTimeMillis() - time));

                        break;
                    }
                    case "NEIGHBOR_INFO": {

                        String localHost = InetAddress.getLocalHost().getHostName();

                        GraphServer.partition.clear();
                        GraphServer.outgoing.clear();

                        // System.out.println("get neighbor");
                        // long time = System.currentTimeMillis();

                        // build partition information
                        int length = in.readInt();

                        HashMap<String, List<String>> tempPartition = new HashMap<>();
                        Serialization.deserialize(getBuffer(in, length), tempPartition);

                        // System.out.println("get neighbor time " + (System.currentTimeMillis() - time));

                        for (Map.Entry<String, List<String>> e : tempPartition.entrySet()) {
                            String host = e.getValue().get(0).split("#")[1];
                            int i = Integer.parseInt(e.getKey());
                            GraphServer.partition.put(i, host);
                            // build outgoing list
                            if (!GraphServer.outgoing.containsKey(host)) {
                                GraphServer.outgoing.put(host, new HashMap<>());
                            }
                            if (!GraphServer.graph.containsKey(i) && !GraphServer.outgoing.get(host).containsKey(i)) {
                                GraphServer.outgoing.get(host).put(i, new ArrayList<>());
                            }
                        }

                        GraphServer.outgoing.remove(localHost);
                        GraphServer.vms = GraphServer.outgoing.size();
                        // System.out.println("Neighbor vms " + GraphServer.vms);

                        scatter();
                        out.writeUTF("DONE");
                        out.flush();

                        GraphServer.isInitialized = true;
                        GraphServer.iterationDone = true;

                        break;
                    }
                    case "SSSP": {

                        while (!GraphServer.iterationDone) {
                            try {
                                Thread.sleep(1);
                            } catch (Exception e) {
                                // do nothing
                            }
                        }

                        GraphServer.iterationDone = false;
                        GraphServer.isInitialized = false;
                        GraphServer.iterations = 0;
                        GraphServer.threshold = 0;
                        GraphServer.graph.clear();
                        GraphServer.incoming.clear();
                        GraphServer.incomeCache.clear();
                        GraphServer.graphApplication = new SSSP();

                        out.writeUTF("DONE");
                        out.flush();

                        GraphServer.iterationDone = true;

                        break;
                    }
                    case "PAGERANK": {

                        while (!GraphServer.iterationDone) {
                            try {
                                Thread.sleep(1);
                            } catch (Exception e) {
                                // do nothing
                            }
                        }

                        GraphServer.iterationDone = false;
                        GraphServer.isInitialized = false;
                        GraphServer.iterations = 0;
                        GraphServer.graph.clear();
                        GraphServer.incoming.clear();
                        GraphServer.incomeCache.clear();

                        int num = in.readInt();
                        GraphServer.graphApplication = new PageRank(in.readDouble());
                        if (num > 1) {
                            GraphServer.threshold = in.readDouble();
                        } else {
                            GraphServer.threshold = 0;
                        }

                        out.writeUTF("DONE");
                        out.flush();
                        GraphServer.iterationDone = true;

                        break;
                    }
                    case "ITERATION": {

                        GraphServer.iterationDone = false;

                        long time = System.currentTimeMillis();
                        DaemonHelper.writeLog("ITERATION", "");

                        // Gather
                        synchronized (GraphServer.incomeCache) {
                            for (HashMap<Integer, List<Double>> hm : GraphServer.incomeCache) {
                                for (Map.Entry<Integer, List<Double>> e : hm.entrySet()) {
                                    GraphServer.incoming.get(e.getKey()).addAll(e.getValue());
                                }
                            }
                            GraphServer.incomeCache.clear();
                        }

                        GraphServer.iterations++;
                        System.out.println("gather time " + (System.currentTimeMillis() - time));
                        time = System.currentTimeMillis();

                        // Apply
                        boolean isFinish = true;
                        for (Vertex v : GraphServer.graph.values()) {
                            double newValue = GraphServer.graphApplication.apply(v, GraphServer.incoming.get(v.getID()));
                            if (isFinish && Math.abs(newValue - v.getValue()) > GraphServer.threshold)
                                isFinish = false;
                            v.setValue(newValue);
                            GraphServer.incoming.get(v.getID()).clear();
                        }

                        System.out.println("apply time " + (System.currentTimeMillis() - time));

                        // Scatter
                        scatter();

                        // iteration done
                        GraphServer.iterationDone = true;
                        if (!isFinish)
                            System.out.println("ITERATION DONE " + GraphServer.iterations);
                        else
                            System.out.println("ITERATION HALT HALT " + GraphServer.iterations);
                        DaemonHelper.writeLog("ITERATION DONE", "");

                        if (isFinish)
                            out.writeUTF("HALT");
                        else
                            out.writeUTF("DONE");
                        out.flush();
                        break;
                    }
                    case "put": {

                        // get scatter info
                        int length = in.readInt();

                        ByteBuffer bb = getBuffer(in, length);

                        out.writeUTF("done");
                        out.flush();

                        synchronized (GraphServer.incomeCache) {
                            GraphServer.incomeCache.add(deserializeHashMap(bb, length));
                        }
                        return;
                    }
                    case "TERMINATE": {

                        // return result to maser
                        List<String> results = new ArrayList<>(GraphServer.graph.size());
                        List<Vertex> vertices = new ArrayList<>(GraphServer.graph.values());
                        vertices.sort((v1, v2) -> Double.compare(v2.getValue(), v1.getValue()));
                        for (Vertex v : vertices) {
                            Formatter f = new Formatter();
                            results.add(v.getID() + "," + f.format("%.5f", v.getValue()));
                        }

                        int length = Serialization.byteCount(results);
                        out.writeInt(length);
                        out.flush();

                        ByteBuffer b = Serialization.serialize(results, length);
                        b.clear();

                        byte[] buffer = new byte[Daemon.bufferSize];
                        while (length > 0) {
                            b.get(buffer, 0, Math.min(Daemon.bufferSize, length));
                            out.write(buffer, 0, Math.min(Daemon.bufferSize, length));
                            out.flush();
                            length -= Daemon.bufferSize;
                        }

                        return;
                    }
                    case "NEW_MASTER": {

                        // new master request info
                        if (!GraphServer.isInitialized) {
                            out.writeInt(0);
                            out.flush();
                            break;
                        }

                        while (!GraphServer.iterationDone) {
                            try {
                                Thread.sleep(1);
                            } catch (Exception e) {
                                // do nothing
                            }
                        }

                        out.writeInt(1);
                        out.flush();
                        out.writeInt(GraphServer.iterations);
                        out.flush();

                        break;
                    }
                }
            }
        } catch (Exception e) {
            GraphServer.iterationDone = true;
            e.printStackTrace();
        }
    }

    private void scatter() {

        long time = System.currentTimeMillis();

        for (Vertex v : GraphServer.graph.values()) {
            double scatterValue = GraphServer.graphApplication.scatter(v);
            for (int i : v.neighbors) {
                if (GraphServer.incoming.containsKey(i)) {
                    GraphServer.incoming.get(i).add(scatterValue);
                } else {
                    GraphServer.outgoing.get(GraphServer.partition.get(i)).get(i).add(scatterValue);
                }
            }
        }

        System.out.println("put cal time " + (System.currentTimeMillis() - time));
        time = System.currentTimeMillis();

        int[] putCount = {0};
        for (Map.Entry<String, HashMap<Integer, List<Double>>> e : GraphServer.outgoing.entrySet()) {
            Thread t = new SendGraph(e.getKey(), e.getValue(), putCount);
            t.start();
        }

        while (putCount[0] != GraphServer.vms) {
            try {
                Thread.sleep(1);
            } catch (Exception e) {
                // do nothing
            }
        }

        System.out.println("total scatter time " + (System.currentTimeMillis() - time));
    }

    private class SendGraph extends Thread {

        String target;
        HashMap<Integer, List<Double>> map;
        int[] putCount;

        SendGraph(String target, HashMap<Integer, List<Double>> map, int[] putCount) {
            this.target = target;
            this.map = map;
            this.putCount = putCount;
        }

        @Override
        public void run() {

            try (Socket socket = new Socket(target, Daemon.graphPortNumber);
                 ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                 ObjectInputStream in = new ObjectInputStream(socket.getInputStream())
            ) {

                out.writeUTF("put");
                out.flush();

                int size = 0;
                for (Map.Entry<Integer, List<Double>> e : map.entrySet()) {
                    size += 8 + e.getValue().size() * 8;
                }

                ByteBuffer b = serializeHashMap(map, size);
                b.clear();
                out.writeInt(size);
                out.flush();
                byte[] buffer = new byte[Daemon.bufferSize];
                while (size > 0) {
                    b.get(buffer, 0, Math.min(Daemon.bufferSize, size));
                    out.write(buffer, 0, Math.min(Daemon.bufferSize, size));
                    out.flush();
                    size -= Daemon.bufferSize;
                }
                in.readUTF();

                // clear outgoing info
                for (int i : GraphServer.outgoing.get(target).keySet()) {
                    GraphServer.outgoing.get(target).get(i).clear();
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
            putCount[0]++;
        }
    }

    private ByteBuffer getBuffer(ObjectInputStream in, int length) {
        int bytes;

        ByteBuffer bb = ByteBuffer.allocate(length);
        byte[] buffer = new byte[Daemon.bufferSize];
        try {
            while (length > 0 && (bytes = in.read(buffer, 0, Math.min(Daemon.bufferSize, length))) != -1) {
                bb.put(buffer, 0, Math.min(Daemon.bufferSize, bytes));
                length -= bytes;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return bb;
    }

    private ByteBuffer serializeHashMap(HashMap<Integer, List<Double>> map, int size) {

        ByteBuffer bb = ByteBuffer.allocate(size);
        for (Map.Entry<Integer, List<Double>> e : map.entrySet()) {
            bb.putInt(e.getKey());
            bb.putInt(e.getValue().size());
            for (double d : e.getValue())
                bb.putDouble(d);
        }
        return bb;
    }

    private HashMap<Integer, List<Double>> deserializeHashMap(ByteBuffer bb, int length) {

        HashMap<Integer, List<Double>> hm = new HashMap<>();
        int index = 0;
        bb.clear();
        while (index < length) {
            int id = bb.getInt();
            int num = bb.getInt();
            index += 8;
            List<Double> list = new ArrayList<>(num);
            while (num > 0) {
                double ddd = bb.getDouble();
                list.add(ddd);
                index += 8;
                num--;
            }
            hm.put(id, list);
        }
        return hm;
    }
}
