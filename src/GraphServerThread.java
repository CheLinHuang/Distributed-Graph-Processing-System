import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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
                    case "add": {

                        int num = in.readInt();

                        // build local graph
                        while (num > 0) {
                            Vertex v = (Vertex) in.readObject();
                            GraphServer.graph.put(v.ID, v);
                            GraphServer.incoming.put(v.ID, new ArrayList<>());
                            num--;
                        }
                        out.writeUTF("done");
                        out.flush();
                        break;
                    }
                    case "neighbor info": {

                        GraphServer.partition = new HashMap<>();
                        GraphServer.outgoing = new HashMap<>();

                        int neighbor = in.readInt();

                        // build partition information
                        while (neighbor > 0) {
                            GraphServer.partition.put(in.readInt(), in.readUTF());
                            neighbor--;
                        }

                        // build outgoing list
                        for (String s : GraphServer.partition.values()) {
                            if (!GraphServer.outgoing.containsKey(s)) {
                                GraphServer.outgoing.put(s, new HashMap<>());
                            }
                        }
                        GraphServer.vms = GraphServer.outgoing.size();

                        push();
                        while (GraphServer.gatherCount != GraphServer.vms) {
                        }

                        GraphServer.isInitialized = true;
                        GraphServer.iterationDone = true;
                        out.writeUTF("done");
                        out.flush();
                        break;
                    }
                    case "delete": {
                        int num = in.readInt();

                        List<Integer> deleteList = new ArrayList<>(num);

                        // build local graph
                        while (num > 0) {
                            int id = in.readInt();
                            out.writeDouble(GraphServer.graph.get(id).value);
                            deleteList.add(id);
                            num--;
                        }
                        out.writeUTF("done");
                        if (in.readUTF().equals("done")) {
                            for (int id : deleteList) {
                                GraphServer.graph.remove(id);
                                GraphServer.incoming.remove(id);
                            }
                        }
                        out.writeUTF("done");
                        break;
                    }
                    case "init": {

                        // TODO
                        GraphServer.iterationDone = false;
                        GraphServer.isInitialized = false;
                        // init PageRank alpha N iterations
                        // init SSSP iterations
                        GraphServer.graph = new HashMap<>();
                        GraphServer.incoming = new HashMap<>();
                        GraphServer.gatherCount = 0;

                        String function = in.readUTF();
                        GraphServer.isPageRank = function.equals("PageRank");

                        if (GraphServer.isPageRank) {
                            GraphServer.damping = in.readDouble();
                            GraphServer.N = in.readInt();
                        }

                        GraphServer.iterationDone = true;

                        out.writeUTF("done");
                        out.flush();
                        break;
                    }
                    case "iter": {

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
                                    if (GraphServer.isFinish && GraphServer.graph.get(i).value != pr)
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

                        if (GraphServer.isFinish)
                            out.writeUTF("finish");
                        else
                            out.writeUTF("done");
                        out.flush();
                        break;
                    }
                    case "put": {
                        int num = in.readInt();

                        while (!GraphServer.iterationDone) {
                        }

                        while (num > 0) {
                            Map.Entry<Integer, List<Double>> e = (Map.Entry<Integer, List<Double>>) in.readObject();
                            GraphServer.incoming.get(e.getKey()).addAll(e.getValue());
                            num--;
                        }

                        GraphServer.gatherCount++;
                        return;
                    }
                    case "finish": {
                        out.writeInt(GraphServer.graph.size());
                        out.flush();
                        for (Vertex v : GraphServer.graph.values()) {
                            out.writeInt(v.ID);
                            out.writeDouble(v.value);
                        }
                        out.writeUTF("done");
                        out.flush();
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
                                out.writeUTF("finish");
                            else {
                                out.writeUTF("done");
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
                out.writeInt(map.size());
                for (Map.Entry<Integer, List<Double>> e : map.entrySet())
                    out.writeObject(e);
            } catch (Exception e) {
                e.printStackTrace();
            }
            putCount[0]++;
        }
    }
}
