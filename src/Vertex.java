import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Vertex implements Serializable {
    int ID;
    List<Integer> neighbors;
    double value;

    public Vertex(int ID) {
        this.ID = ID;
        neighbors = new ArrayList<>();
    }
}
