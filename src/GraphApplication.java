import java.util.Collection;

// Generic graph application interface
public interface GraphApplication {

    double apply(Vertex v, Collection<Double> list);

    double scatter(Vertex v);
}
