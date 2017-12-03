import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class CollectResults extends Thread {

    private List<List<String>> results;
    private List<Boolean> flags;
    private ObjectInputStream in;

    public CollectResults(
            List<List<String>> results,
            List<Boolean> flags,
            ObjectInputStream in) {
        this.results = results;
        this.flags = flags;
        this.in = in;
    }

    @Override
    public void run() {
        try {
            List<String> subGraph = new ArrayList<>();
            byte[] buffer = new byte[Daemon.bufferSize];
            int size = in.readInt(), bytes;
            ByteBuffer bb = ByteBuffer.allocate(size);
            while (size > 0 &&
                    (bytes = in.read(buffer, 0, Math.min(Daemon.bufferSize, size))) != -1) {
                bb.put(buffer, 0, Math.min(Daemon.bufferSize, bytes));
                size -= bytes;
            }
            Serialization.deserialize(bb, subGraph);
            synchronized (results) {
                results.add(subGraph);
            }
            synchronized (flags) {
                flags.add(false);
            }
        } catch (Exception e) {
            e.printStackTrace();
            flags.add(true);
        }
    }
}