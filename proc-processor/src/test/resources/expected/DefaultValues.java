package good;

import org.neo4j.graphalgo.core.CypherMapWrapper;

import javax.annotation.processing.Generated;

@Generated("org.neo4j.graphalgo.proc.ConfigurationProcessor")
public final class DefaultValuesConfig implements DefaultValues {

    private final int defaultInt;

    private final String defaultString;

    public DefaultValuesConfig(CypherMapWrapper config) {
        this.defaultInt = config.getInt("defaultInt", DefaultValues.super.defaultInt());
        this.defaultString = config.getString("defaultString", DefaultValues.super.defaultString());
    }

    @Override
    public int defaultInt() {
        return this.defaultInt;
    }

    @Override
    public String defaultString() {
        return this.defaultString;
    }
}
