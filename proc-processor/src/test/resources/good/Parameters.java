package good;

import org.neo4j.graphalgo.annotation.Configuration;

@Configuration("ParametersConfig")
public interface Parameters {

    @Configuration.Parameter
    int keyFromParameter();

    long keyFromMap();

    @Configuration.Parameter
    int parametersAreAddedFirst();
}
