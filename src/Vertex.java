import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Vertex implements Serializable {
    private int ID;
    List<Integer> neighbors;
    private double value;

    public Vertex(int ID, double value) {
        this.ID = ID;
        this.value = value;
        neighbors = new ArrayList<>();
    }

    public void addNeighbor(int neighbor) {
        neighbors.add(neighbor);
    }

    public int getID() {
        return this.ID;
    }

    public void setValue(double newValue) {
        this.value = newValue;
    }

    public double getValue() {
        return this.value;
    }

    @Override
    public String toString() {
        String output = "ID: " + this.ID + "/Value: " + this.value + "/Neighbors: ";
        for (Integer neighbor : neighbors)
            output += neighbor + ", ";
        return output.substring(0, output.length() - 2);
    }
}
