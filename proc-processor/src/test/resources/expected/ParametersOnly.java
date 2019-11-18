package good;

import javax.annotation.Generated;

@Generated("org.neo4j.graphalgo.proc.ConfigurationProcessor")
public final class ParametersOnlyConfig implements ParametersOnly {

    private final int onlyAsParameter;

    public ParametersOnlyConfig(int onlyAsParameter) {
        this.onlyAsParameter = onlyAsParameter;
    }

    @Override
    public int onlyAsParameter() {
        return this.onlyAsParameter;
    }
}
