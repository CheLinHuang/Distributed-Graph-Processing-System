import java.util.Collection;

public class SSSP implements GraphApplication {

    @Override
    public double apply(Vertex v, Collection<Double> list) {
        double min = Double.MAX_VALUE;
        for (double value : list) {
            if (value < min)
                min = value;
        }
        return Math.min(min + 1, v.getValue());
    }

    @Override
    public double scatter(Vertex v) {
        return v.getValue();
    }
}
