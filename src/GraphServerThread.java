import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
                System.out.println(operation);

                switch (operation) {
                    case "ADD": {

                        // build local graph
//                        Vertex v = (Vertex) in.readObject();
//                        GraphServer.graph.put(v.ID, v);
//                        GraphServer.incoming.put(v.ID, new ArrayList<>());

                        // build local graph in one pass
                        long num = in.readLong();
                        while (num > 0) {
                            Vertex v = (Vertex) in.readObject();
                            GraphServer.graph.put(v.getID(), v);
                            GraphServer.incoming.put(v.getID(), new ArrayList<>());
                            num--;
                        }
                        break;

                    }
                    case "NEIGHBOR_INFO": {

                        String localHost = InetAddress.getLocalHost().getHostName();

                        GraphServer.partition.clear();
                        GraphServer.outgoing.clear();

                        System.out.println("get neighbor");

                        // build partition information
                        GraphServer.partition.putAll((HashMap<Integer, String>) in.readObject());
                        for (int i : GraphServer.partition.keySet()) {
                            String host = GraphServer.partition.get(i).split("#")[1];
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
                        System.out.println("Neighbor vms " + GraphServer.vms);

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

                        System.out.println("# of param " + in.readInt());
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

                            }
                        }

                        GraphServer.iterationDone = false;
                        GraphServer.isInitialized = false;
                        GraphServer.iterations = 0;
                        GraphServer.graph.clear();
                        GraphServer.incoming.clear();
                        GraphServer.incomeCache.clear();

                        int num = in.readInt();
                        System.out.println("# of param " + num);
                        GraphServer.graphApplication = new PageRank(in.readDouble());
                        if (num > 1) {
                            GraphServer.threshold = in.readDouble();
                            System.out.println("Get threshold " + GraphServer.threshold);
                        } else {
                            GraphServer.threshold = 0;
                        }

                        out.writeUTF("DONE");
                        out.flush();
                        GraphServer.iterationDone = true;

                        break;
                    }
                    case "ITERATION": {

                        GraphServer.iterations++;
                        GraphServer.iterationDone = false;
                        boolean isFinish = true;

                        long time = System.currentTimeMillis();

                        // Gather
                        for (HashMap<Integer, List<Double>> hm : GraphServer.incomeCache) {
                            for (Map.Entry<Integer, List<Double>> e : hm.entrySet()) {
                                GraphServer.incoming.get(e.getKey()).addAll(e.getValue());
                            }
                        }
                        GraphServer.incomeCache.clear();

                        System.out.println("gather time " + (System.currentTimeMillis() - time));
                        time = System.currentTimeMillis();

                        // Apply

                        for (Vertex v : GraphServer.graph.values()) {
                            double newValue = GraphServer.graphApplication.apply(v, GraphServer.incoming.get(v.getID()));
                            if (isFinish && Math.abs(newValue - v.getValue()) > GraphServer.threshold)
                                isFinish = false;
                            v.setValue(newValue);
                            GraphServer.incoming.get(v.getID()).clear();
                        }

                        System.out.println("apply time " + (System.currentTimeMillis() - time));

                        scatter();

                        // iteration done
                        GraphServer.iterationDone = true;
                        System.out.println("iteration done");

                        if (isFinish)
                            out.writeUTF("HALT");
                        else
                            out.writeUTF("DONE");
                        out.flush();
                        break;
                    }
                    case "put": {

                        long time = System.currentTimeMillis();

                        synchronized (GraphServer.incomeCache) {
                            System.out.println("wait time for synchronized" + (System.currentTimeMillis() - time));
                            time = System.currentTimeMillis();
                            GraphServer.incomeCache.add((HashMap<Integer, List<Double>>) in.readObject());

                            System.out.println("time for send file" + (System.currentTimeMillis() - time));
                        }
                        out.writeUTF("done");
                        out.flush();
                        return;
                    }
                    case "TERMINATE": {

                        out.writeInt(GraphServer.graph.size());
                        out.flush();
                        for (Vertex v : GraphServer.graph.values()) {
                            out.writeInt(v.getID());
                            out.flush();
                            out.writeDouble(v.getValue());
                            out.flush();
                        }
                        return;
                    }
                    case "NEW_MASTER": {
                        if (!GraphServer.isInitialized) {
                            out.writeInt(0);
                            out.flush();
                            break;
                        }

                        while (!GraphServer.iterationDone) {
                            try {
                                Thread.sleep(1);
                            } catch (Exception e) {

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
            for (int i : v.neighbors) {
                if (GraphServer.incoming.containsKey(i)) {
                    GraphServer.incoming.get(i).add(GraphServer.graphApplication.scatter(v));
                } else {
                    GraphServer.outgoing.get(GraphServer.partition.get(i)).get(i).add(GraphServer.graphApplication.scatter(v));
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
                Thread.sleep(10);
            } catch (Exception e) {

            }
        }

        System.out.println("total scatter time " + (System.currentTimeMillis() - time));
    }

    class SendGraph extends Thread {

        String target;
        HashMap<Integer, List<Double>> map;
        int[] putCount;

        SendGraph(String target, HashMap<Integer, List<Double>> map, int[] putCount) {
            this.target = target;
            this.map = map;
            this.putCount = putCount;
        }

        public void run() {
            // long time = System.currentTimeMillis();

            try (Socket socket = new Socket(target, Daemon.graphPortNumber);
                 ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                 ObjectInputStream in = new ObjectInputStream(socket.getInputStream())
            ) {

                out.writeUTF("put");
                out.flush();
                out.writeObject(map);
                out.flush();
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
}
