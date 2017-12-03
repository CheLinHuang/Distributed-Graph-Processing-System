import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.*;

public class Serialization {




    public static int byteCount(List<String> list) {
        int size = 4;
        for (String s: list) {
            size += 4 + s.length();
        }
        return size;
    }

    public static ByteBuffer serialize(List<String> list, int size) {
        ByteBuffer bb = ByteBuffer.allocate(size);
        bb.putInt(list.size());
        for (String s: list) {
            bb.putInt(s.length());
            bb.put(s.getBytes());
        }
        return bb;
    }

    public static void deserialize(ByteBuffer bb, List<String> list) {
        list.clear();
        bb.clear();
        int num = bb.getInt();
        while (num > 0) {
            byte[] bytes = new byte[bb.getInt()];
            bb.get(bytes);
            String s = new String(bytes);
            list.add(s);
            num --;
        }
    }


    public static int byteCount(Map<String, List<String>> map) {
        int size = 0;
        for (Map.Entry<String, List<String>> e: map.entrySet()) {
            size += 8 + e.getKey().length();
            for (String s: e.getValue()) {
                size += 4 + s.length();
            }
        }
        return size;
    }

    public static ByteBuffer serialize(Map<String, List<String>> map, int size) {

        ByteBuffer bb = ByteBuffer.allocate(size);
        for (Map.Entry<String, List<String>> e: map.entrySet()) {
            // System.out.println(e.getKey() + "::" + e.getValue().toString());
            bb.putInt(e.getValue().size() + 1);
            bb.putInt(e.getKey().length());
            bb.put(e.getKey().getBytes());
            for (String s: e.getValue()) {
                bb.putInt(s.length());
                bb.put(s.getBytes());
            }
        }
        return bb;
    }

    public static void deserialize(ByteBuffer bb, Map<String, List<String>> map) {

        map.clear();
        bb.clear();
        while (bb.hasRemaining()) {
            int num = bb.getInt();
            int len = bb.getInt();
            byte[] bytes = new byte[len];
            bb.get(bytes);
            String key = new String(bytes);
            List<String> temp = new ArrayList<>();
            for (int i = 0; i < num - 1; i++) {
                len = bb.getInt();
                bytes = new byte[len];
                bb.get(bytes);
                temp.add(new String(bytes));
            }
            map.put(key, temp);
        }
    }

    public static int byteCount(HashMap<String, long[]> map) {
        int size = 0;
        for (Map.Entry<String, long[]> e: map.entrySet()) {
            size += 8 + e.getKey().length() + 8 * e.getValue().length;
        }
        return size;
    }

    public static ByteBuffer serialize(HashMap<String, long[]> map, int size) {

        ByteBuffer bb = ByteBuffer.allocate(size);
        for (Map.Entry<String, long[]> e: map.entrySet()) {
            bb.putInt(e.getKey().length());
            bb.put(e.getKey().getBytes());
            bb.putInt(e.getValue().length);
            for (long l: e.getValue())
                bb.putLong(l);
        }
        return bb;
    }

    public static void deserialize(ByteBuffer bb, HashMap<String, long[]> map) {
        map.clear();
        bb.clear();
        while (bb.hasRemaining()) {
            byte[] bytes = new byte[bb.getInt()];
            bb.get(bytes);
            String key = new String(bytes);
            int num = bb.getInt();
            long[] value = new long[num];
            for (int i = 0; i < num; i++) {
                value[i] = bb.getLong();
            }
            map.put(key, value);
        }
    }

    public static int byteCountGraph(Map<Integer, Vertex> map) {
        int size = 0;
        for (Map.Entry<Integer, Vertex> e: map.entrySet())
            size += 4 + 8 + 4 + 4 * e.getValue().neighbors.size();
        return size;
    }

    public static ByteBuffer serializeGraph(Map<Integer, Vertex> graph, int size) {
        ByteBuffer bb = ByteBuffer.allocate(size);
        for (Map.Entry<Integer, Vertex> e: graph.entrySet()) {
            Vertex v = e.getValue();
            bb.putInt(v.getID());
            bb.putDouble(v.getValue());
            bb.putInt(v.neighbors.size());
            for (int neighbor: v.neighbors) {
                bb.putInt(neighbor);
            }
        }

        return bb;
    }

    public static void deserializeGraph(ByteBuffer bb, Map<Integer, Vertex> graph) {
        graph.clear();
        bb.clear();
        while(bb.hasRemaining()) {
            int nodeID = bb.getInt();
            double nodeValue = bb.getDouble();
            int num = bb.getInt();
            Vertex v = new Vertex(nodeID, nodeValue);
            while (num > 0) {
                v.addNeighbor(bb.getInt());
                num --;
            }
            graph.put(nodeID, v);
        }
    }

    public static void main(String[] args) {

        List<String> temp = new ArrayList<>();
        temp.add("Winnie");
        temp.add("Wang");
        temp.add("is");
        temp.add("cute");
        System.out.println(temp.toString());
        ByteBuffer bb = serialize(temp, byteCount(temp));
        List<String> list = new ArrayList<>();
        deserialize(bb, list);
        System.out.println(list.toString());

        Map<String, List<String>> map = new HashMap<>();
        Map<String, List<String>> demap = new HashMap<>();
        map.put("Yi-Hsin: ", temp);
        map.put("Ethan: ", temp);

        bb = serialize(map, byteCount(map));
        deserialize(bb, demap);
        System.out.println(map.toString());
        System.out.println(demap.toString());

        HashMap<String, long[]> longMap = new HashMap<>();
        HashMap<String, long[]> deLongMap = new HashMap<>();

        longMap.put("QQWD", new long[] {10, 2, 1});
        longMap.put("JDSX", new long[] {111, 2, 3425});



        bb = serialize(longMap, byteCount(longMap));
        deserialize(bb, deLongMap);
        for (Map.Entry<String, long[]> e: longMap.entrySet()) {
            System.out.println(e.getKey() + "::" + Arrays.toString(e.getValue()));
        }
        for (Map.Entry<String, long[]> e: deLongMap.entrySet()) {
            System.out.println(e.getKey() + "::" + Arrays.toString(e.getValue()));
        }

        Map<Integer, Vertex> graph = new HashMap<>();
        Map<Integer, Vertex> deGraph = new HashMap<>();

        Vertex v1 = new Vertex(1, 100);
        v1.addNeighbor(10);
        Vertex v2 = new Vertex(2, 34);
        v2.addNeighbor(1);
        v2.addNeighbor(9);

        graph.put(1, v1);
        graph.put(2, v2);

        bb = serializeGraph(graph, byteCountGraph(graph));
        deserializeGraph(bb, deGraph);

        for (Map.Entry<Integer, Vertex> e: graph.entrySet())
            System.out.println(e.getValue());

        for (Map.Entry<Integer, Vertex> e: deGraph.entrySet())
            System.out.println(e.getValue());


    }
}
