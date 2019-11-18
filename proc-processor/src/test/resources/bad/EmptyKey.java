package bad;

import org.neo4j.graphalgo.annotation.Configuration;

@Configuration("EmptyKeyConfig")
public interface EmptyKey {

    @Configuration.Key("")
    int emptyKey();

    @Configuration.Key("      ")
    int whitespaceKey();
}
