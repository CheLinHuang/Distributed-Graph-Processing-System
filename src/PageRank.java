import java.util.Collection;

public class PageRank implements GraphApplication{

    private double damping;

    PageRank(double damping) {
        this.damping = damping;
    }

    @Override
    public double apply(Vertex v, Collection<Double> list) {
        double pr = (1 - damping);
        for (double value : list) {
            pr += value * damping;
        }
        return pr;
    }

    @Override
    public double scatter(Vertex v) {
        return v.getValue() / v.neighbors.size();
    }
}
