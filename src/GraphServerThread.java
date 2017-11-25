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
        try (
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream())
        ) {
            String operation = in.readUTF();
            switch (operation) {
                case "init": {

                    GraphServer.iterationDone = false;
                    // init PageRank alpha N iterations
                    // init SSSP iterations
                    GraphServer.graph = new HashMap<>();
                    GraphServer.incoming = new HashMap<>();
                    GraphServer.partition = new HashMap<>();
                    GraphServer.outgoing = new HashMap<>();

                    String function = in.readUTF();
                    GraphServer.isPageRank = function.equals("PageRank");

                    if (GraphServer.isPageRank) {
                        GraphServer.damping = in.readDouble();
                        GraphServer.N = in.readInt();
                    }

                    //GraphServer.iterations = in.readInt();

                    int num = in.readInt();

                    // build local graph
                    while (num > 0) {
                        Vertex v = (Vertex) in.readObject();
                        GraphServer.graph.put(v.ID, v);
                        GraphServer.incoming.put(v.ID, new ArrayList<>());
                        num--;
                    }

                    // build partition information
                    int neighbor = in.readInt();
                    while (neighbor > 0) {
                        GraphServer.partition.put(in.readInt(), in.readUTF());
                        neighbor--;
                    }

                    // build outgoing list
                    for (String s : GraphServer.partition.values()) {
                        if (!GraphServer.outgoing.containsKey(s)) {
                            GraphServer.outgoing.put(s, new ArrayList<>());
                        }
                    }
                    GraphServer.vms = GraphServer.outgoing.size();
                    GraphServer.iterationDone = true;

                    // push
                    push();
                    out.writeUTF("done");
                    break;
                }

                case "iter": {
                    GraphServer.iterationDone = false;

                    // clear outgoing info
                    for (String s : GraphServer.outgoing.keySet()) {
                        GraphServer.outgoing.get(s).clear();
                    }

                    // gather
                    while (GraphServer.gatherCount < GraphServer.vms) {
                        Thread.sleep(100);
                    }

                    // algorithm
                    if (GraphServer.isPageRank) {
                        for (int i : GraphServer.graph.keySet()) {
                            double pr = (1 - GraphServer.damping) / (double) GraphServer.N;
                            for (double value : GraphServer.incoming.get(i)) {
                                pr += value * GraphServer.damping;
                            }
                            GraphServer.graph.get(i).value = pr;
                        }
                    } else {
                        for (int i : GraphServer.graph.keySet()) {
                            double min = Double.MAX_VALUE;
                            for (double value : GraphServer.incoming.get(i)) {
                                if (value < min)
                                    min = value;
                            }
                            GraphServer.graph.get(i).value = Math.min(min + 1, GraphServer.graph.get(i).value);
                        }
                    }

                    // clean incoming info
                    for (int i : GraphServer.incoming.keySet()) {
                        GraphServer.incoming.get(i).clear();
                    }


                    // iteration done
                    GraphServer.iterationDone = true;
                    GraphServer.gatherCount = 0;
                    push();
                    out.writeUTF("done");
                    break;
                }

                case "put": {
                    int num = in.readInt();
                    if (GraphServer.iterationDone) {
                        while (num > 0) {
                            Vertex v = (Vertex) in.readObject();
                            for (int i : v.neighbors) {
                                if (GraphServer.incoming.containsKey(i)) {
                                    if (GraphServer.isPageRank) {
                                        GraphServer.incoming.get(i).add(v.value / v.neighbors.size());
                                    } else {
                                        GraphServer.incoming.get(i).add(v.value);
                                    }
                                }
                            }
                            num--;
                        }

                    } else {
                        List<Vertex> list = new ArrayList<>(num);
                        while (num > 0) {
                            list.add((Vertex) in.readObject());
                            num--;
                        }

                        while (!GraphServer.iterationDone) {
                            Thread.sleep(100);
                        }
                        for (Vertex v : list) {
                            for (int i : v.neighbors) {
                                if (GraphServer.incoming.containsKey(i)) {
                                    if (GraphServer.isPageRank) {
                                        GraphServer.incoming.get(i).add(v.value / v.neighbors.size());
                                    } else {
                                        GraphServer.incoming.get(i).add(v.value);
                                    }
                                }
                            }
                        }
                    }
                    GraphServer.gatherCount++;
                    break;
                }

                case "collect": {
                    out.writeInt(GraphServer.graph.size());
                    for (Vertex v : GraphServer.graph.values())
                        out.writeObject(v);
                    break;
                }
            }
        } catch (Exception e) {
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
                    GraphServer.outgoing.get(GraphServer.partition.get(i)).add(v);
                }
            }
        }

        int[] putCount = {0};
        for (Map.Entry<String, List<Vertex>> e : GraphServer.outgoing.entrySet()) {
            Thread t = new SendGraph(e.getKey(), e.getValue(), putCount);
            t.start();
        }
        while (putCount[0] != GraphServer.vms) {
            try {
                Thread.sleep(100);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    class SendGraph extends Thread {

        String target;
        List<Vertex> list;
        int[] putCount;

        SendGraph(String target, List<Vertex> list, int[] putCount) {
            this.target = target;
            this.list = list;
            this.putCount = putCount;
        }

        public void run() {
            try (Socket socket = new Socket(target, Daemon.graphPortNumber);
                 ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())
            ) {
                out.writeInt(list.size());
                for (Vertex v : list)
                    out.writeObject(v);
            } catch (Exception e) {
                e.printStackTrace();
            }
            putCount[0]++;
        }
    }
}
