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

        String operation = null;

        try (
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream())
        ) {

            while (true) {

                operation = in.readUTF();
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
                            GraphServer.graph.put(v.ID, v);
                            GraphServer.incoming.put(v.ID, new ArrayList<>());
                            num--;
                        }
                        break;

                    }
                    case "NEIGHBOR_INFO": {

                        String localHost = InetAddress.getLocalHost().getHostName();

                        GraphServer.partition = new HashMap<>();
                        GraphServer.outgoing = new HashMap<>();

                        System.out.println("get neighbor");

                        // build partition information
                        GraphServer.partition = (HashMap<Integer, String>) in.readObject();
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

                        push();

                        GraphServer.isInitialized = true;
                        GraphServer.iterationDone = true;
                        out.writeUTF("DONE");
                        out.flush();
                        break;
                    }
                    case "DELETE": {

                        // build local graph
                        int id = in.readInt();
                        out.writeObject(GraphServer.graph.get(id));
                        if (in.readUTF().equals("DONE")) {
                            GraphServer.graph.remove(id);
                            GraphServer.incoming.remove(id);
                        }
                        break;
                    }
                    case "SSSP": {

                        GraphServer.iterationDone = false;
                        GraphServer.isInitialized = false;
                        GraphServer.isPageRank = false;
                        GraphServer.graph = new HashMap<>();
                        GraphServer.incoming = new HashMap<>();
                        GraphServer.incomeCache = new ArrayList<>();

                        System.out.println("# of param " + in.readInt());
                        GraphServer.iterationDone = true;

                        out.writeUTF("DONE");
                        out.flush();

                        break;
                    }
                    case "PAGERANK": {

                        GraphServer.iterationDone = false;
                        GraphServer.isInitialized = false;
                        GraphServer.isPageRank = true;
                        GraphServer.graph = new HashMap<>();
                        GraphServer.incoming = new HashMap<>();
                        GraphServer.incomeCache = new ArrayList<>();

                        int num = in.readInt();
                        System.out.println("# of param " + num);
                        GraphServer.damping = in.readDouble();
                        System.out.println("Get damping " + GraphServer.damping);
                        if (num > 1) {
                            GraphServer.threshold = in.readDouble();
                            System.out.println("Get threshold " + GraphServer.threshold);
                        } else {
                            GraphServer.threshold = 0;
                        }
                        GraphServer.iterationDone = true;

                        out.writeUTF("DONE");
                        out.flush();

                        break;
                    }
                    case "ITERATION": {

                        GraphServer.iterationDone = false;
                        GraphServer.isFinish = true;

                        long time = System.currentTimeMillis();

                        // gather
                        for (HashMap<Integer, List<Double>> hm : GraphServer.incomeCache) {
                            for (Map.Entry<Integer, List<Double>> e : hm.entrySet()) {
                                GraphServer.incoming.get(e.getKey()).addAll(e.getValue());
                            }
                        }
                        GraphServer.incomeCache.clear();

                        System.out.println("gather time " + (System.currentTimeMillis() - time));
                        time = System.currentTimeMillis();

                        // algorithm
                        if (GraphServer.isPageRank) {
                            for (int i : GraphServer.graph.keySet()) {
                                //System.out.println("calculate id: " + i);
                                double pr = (1 - GraphServer.damping);
                                for (double value : GraphServer.incoming.get(i)) {
                                    //System.out.println("income value " + value);
                                    pr += value * GraphServer.damping;
                                }
                                if (GraphServer.isFinish && Math.abs(GraphServer.graph.get(i).value - pr) > GraphServer.threshold)
                                    GraphServer.isFinish = false;
                                GraphServer.graph.get(i).value = pr;
                                GraphServer.incoming.get(i).clear();
                            }
                        } else {
                            for (int i : GraphServer.graph.keySet()) {
                                double min = Double.MAX_VALUE;
                                for (double value : GraphServer.incoming.get(i)) {
                                    if (value < min)
                                        min = value;
                                }
                                if (GraphServer.isFinish && GraphServer.graph.get(i).value != Math.min(min + 1, GraphServer.graph.get(i).value))
                                    GraphServer.isFinish = false;
                                GraphServer.graph.get(i).value = Math.min(min + 1, GraphServer.graph.get(i).value);
                                GraphServer.incoming.get(i).clear();
                            }
                        }

                        System.out.println("algorithm time " + (System.currentTimeMillis() - time));

                        push();

                        // iteration done
                        GraphServer.iterationDone = true;
                        System.out.println("iteration done");

                        if (GraphServer.isFinish)
                            out.writeUTF("HALT");
                        else
                            out.writeUTF("DONE");
                        out.flush();
                        break;
                    }
                    case "put": {

                        HashMap<Integer, List<Double>> temp = (HashMap<Integer, List<Double>>) in.readObject();
                        synchronized (GraphServer.incomeCache) {
                            GraphServer.incomeCache.add(temp);
                        }
                        out.writeUTF("done");
                        out.flush();
                        return;

                    }
                    case "TERMINATE": {
                        out.writeInt(GraphServer.graph.size());
                        out.flush();
                        for (Vertex v : GraphServer.graph.values()) {
                            out.writeInt(v.ID);
                            out.flush();
                            out.writeDouble(v.value);
                            out.flush();
                        }
                        return;
                    }
                    case "new Master": {
                        if (!GraphServer.isInitialized) {
                            out.writeUTF("REINITIALIZE");
                        } else if (GraphServer.needResend) {
                            out.writeUTF("RESEND");
                        } else {
                            while (!GraphServer.iterationDone) {
                            }
                            if (GraphServer.isFinish)
                                out.writeUTF("HALT");
                            else {
                                out.writeUTF("DONE");
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            if (operation != null) {
                GraphServer.needResend = GraphServer.isInitialized && (operation.equals("add") || operation.equals("delete") || operation.equals("neighbor info"));
            }
            GraphServer.iterationDone = true;
            e.printStackTrace();
        }
    }

    private void push() {

        long time = System.currentTimeMillis();

        if (GraphServer.isPageRank) {
            for (Vertex v : GraphServer.graph.values()) {
                for (int i : v.neighbors) {
                    if (GraphServer.incoming.containsKey(i)) {
                        GraphServer.incoming.get(i).add(v.value / v.neighbors.size());
                    } else {
                        GraphServer.outgoing.get(GraphServer.partition.get(i)).get(i).add(v.value / v.neighbors.size());
                    }
                }
            }
        } else {
            for (Vertex v : GraphServer.graph.values()) {
                for (int i : v.neighbors) {
                    if (GraphServer.incoming.containsKey(i)) {
                        GraphServer.incoming.get(i).add(v.value);
                    } else {
                        GraphServer.outgoing.get(GraphServer.partition.get(i)).get(i).add(v.value);
                    }
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
                in.readUTF();

                //System.out.println(this.getId() + " scatter time " + (System.currentTimeMillis() - time));

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
