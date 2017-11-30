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

            String localHost = InetAddress.getLocalHost().getHostName();

            while (true) {

                operation = in.readUTF();
                System.out.println(operation);

                switch (operation) {
                    case "ADD": {

                        // build local graph
                        Vertex v = (Vertex) in.readObject();
                        GraphServer.graph.put(v.ID, v);
                        GraphServer.incoming.put(v.ID, new ArrayList<>());

                        System.out.println("Add vertex " + v.toString());

                        break;
                    }
                    case "NEIGHBOR_INFO": {

                        GraphServer.partition = new HashMap<>();
                        GraphServer.outgoing = new HashMap<>();

                        System.out.println("get neighbor");

                        // build partition information
                        GraphServer.partition = (HashMap<Integer, String>) in.readObject();
                        for (int i : GraphServer.partition.keySet()) {
                            String host = GraphServer.partition.get(i).split("#")[1];
                            GraphServer.partition.put(i, host);
                            System.out.println(i + " " + host);
                        }


                        // build outgoing list
                        for (String s : GraphServer.partition.values()) {
                            if (!GraphServer.outgoing.containsKey(s)) {
                                GraphServer.outgoing.put(s, new HashMap<>());
                            }
                        }
                        GraphServer.outgoing.remove(localHost);
                        GraphServer.vms = GraphServer.outgoing.size();
                        System.out.println("Neighbor vms " + GraphServer.vms);

                        push();
                        while (GraphServer.gatherCount != GraphServer.vms) {
                        }

                        GraphServer.isInitialized = true;
                        GraphServer.iterationDone = true;
                        out.writeUTF("DONE");
                        out.flush();
                        break;
                    }
                    case "DELETE": {

                        // build local graph
                        int id = in.readInt();
                        System.out.println("Delete vertex " + id);
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
                        GraphServer.gatherCount = 0;

                        System.out.println("# of param " + in.readInt());
                        GraphServer.iterationDone = true;

                        out.writeUTF("DONE");
                        out.flush();

                        break;
                    }
                    case "PAGERANK": {

                        // TODO
                        GraphServer.iterationDone = false;
                        GraphServer.isInitialized = false;
                        GraphServer.isPageRank = true;
                        GraphServer.graph = new HashMap<>();
                        GraphServer.incoming = new HashMap<>();
                        GraphServer.gatherCount = 0;

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
                        GraphServer.gatherCount = 0;
                        GraphServer.isFinish = true;

                        // algorithm
                        synchronized (GraphServer.incoming) {
                            if (GraphServer.isPageRank) {
                                for (int i : GraphServer.graph.keySet()) {
                                    double pr = (1 - GraphServer.damping);
                                    for (double value : GraphServer.incoming.get(i)) {
                                        pr += value * GraphServer.damping;
                                    }
                                    if (GraphServer.isFinish && Math.abs(GraphServer.graph.get(i).value - pr) > GraphServer.threshold)
                                        GraphServer.isFinish = false;
                                    GraphServer.graph.get(i).value = pr;
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
                                }
                            }

                            // clean incoming info
                            for (int i : GraphServer.incoming.keySet()) {
                                GraphServer.incoming.get(i).clear();
                            }
                        }

                        push();

                        // iteration done
                        GraphServer.iterationDone = true;

                        // gather
                        while (GraphServer.gatherCount < GraphServer.vms) {
                        }

                        System.out.println("iteration done");

                        if (GraphServer.isFinish)
                            out.writeUTF("HALT");
                        else
                            out.writeUTF("DONE");
                        out.flush();
                        break;
                    }
                    case "put": {

                        HashMap<Integer, List<Double>> e = (HashMap<Integer, List<Double>>) in.readObject();

                        System.out.println("GraphServer.iterationDone :" + GraphServer.iterationDone);

                        while (!GraphServer.iterationDone) {
                        }
                        for (Map.Entry<Integer, List<Double>> entry : e.entrySet()) {
                            GraphServer.incoming.get(entry.getKey()).addAll(entry.getValue());
                        }
                        GraphServer.gatherCount++;
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
        for (Vertex v : GraphServer.graph.values()) {
            for (int i : v.neighbors) {
                if (GraphServer.incoming.containsKey(i)) {
                    if (GraphServer.isPageRank) {
                        GraphServer.incoming.get(i).add(v.value / v.neighbors.size());
                    } else {
                        GraphServer.incoming.get(i).add(v.value);
                    }
                } else {
                    List<Double> list = GraphServer.outgoing.get(GraphServer.partition.get(i)).getOrDefault(i, new ArrayList<>());
                    if (GraphServer.isPageRank) {
                        list.add(v.value / v.neighbors.size());
                    } else {
                        list.add(v.value);
                    }
                    if (!GraphServer.outgoing.get(GraphServer.partition.get(i)).containsKey(i))
                        GraphServer.outgoing.get(GraphServer.partition.get(i)).put(i, list);
                }
            }
        }

        int[] putCount = {0};
        for (Map.Entry<String, HashMap<Integer, List<Double>>> e : GraphServer.outgoing.entrySet()) {
            Thread t = new SendGraph(e.getKey(), e.getValue(), putCount);
            t.start();
        }

        // clear outgoing info
        for (String s : GraphServer.outgoing.keySet()) {
            GraphServer.outgoing.get(s).clear();
        }
        System.out.println("Pushing");
        while (putCount[0] != GraphServer.vms) {
        }
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
            try (Socket socket = new Socket(target, Daemon.graphPortNumber);
                 ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())
            ) {
                out.writeUTF("put");
                out.flush();
                out.writeObject(map);

            } catch (Exception e) {
                e.printStackTrace();
            }
            putCount[0]++;
        }
    }
}
