package good;

import org.neo4j.graphalgo.annotation.Configuration;

@Configuration("ParametersOnlyConfig")
public interface ParametersOnly {

    @Configuration.Parameter
    int onlyAsParameter();
}
