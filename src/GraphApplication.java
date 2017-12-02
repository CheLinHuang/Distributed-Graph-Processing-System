import java.util.Collection;

public interface GraphApplication {

    double apply(Vertex v, Collection<Double> list);

    double scatter(Vertex v);
}
