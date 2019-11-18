package good;

import org.neo4j.graphalgo.annotation.Configuration;

@Configuration("DefaultValuesConfig")
public interface DefaultValues {

    default int defaultInt() {
        return 42;
    }

    default String defaultString() {
        return "foo";
    }
}
