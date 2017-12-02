import java.util.List;

public interface GraphApplication {

    double apply(Vertex v, List<Double> list);

    double scatter(Vertex v);
}
