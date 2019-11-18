package good;

import org.neo4j.graphalgo.core.CypherMapWrapper;

import javax.annotation.Generated;

@Generated("org.neo4j.graphalgo.proc.ConfigurationProcessor")
public final class ParametersConfig implements Parameters {

    private final int keyFromParameter;

    private final long keyFromMap;

    private final int parametersAreAddedFirst;

    public ParametersConfig(int keyFromParameter, int parametersAreAddedFirst, CypherMapWrapper config) {
        this.keyFromParameter = keyFromParameter;
        this.keyFromMap = config.requireLong("keyFromMap");
        this.parametersAreAddedFirst = parametersAreAddedFirst;
    }

    @Override
    public int keyFromParameter() {
        return this.keyFromParameter;
    }

    @Override
    public long keyFromMap() {
        return this.keyFromMap;
    }

    @Override
    public int parametersAreAddedFirst() {
        return this.parametersAreAddedFirst;
    }
}
