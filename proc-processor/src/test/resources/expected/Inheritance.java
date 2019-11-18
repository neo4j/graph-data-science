package good;

import org.neo4j.graphalgo.core.CypherMapWrapper;

import javax.annotation.processing.Generated;

@Generated("org.neo4j.graphalgo.proc.ConfigurationProcessor")
public final class MyConfig implements Inheritance.MyConfig {

    private final String baseValue;

    private final int overriddenValue;

    private final long overwrittenValue;

    private final double inheritedValue;

    private final short inheritedDefaultValue;

    public MyConfig(CypherMapWrapper config) {
        this.baseValue = config.requireString("baseValue");
        this.overriddenValue = config.getInt("overriddenValue", Inheritance.MyConfig.super.overriddenValue());
        this.overwrittenValue = config.getLong("overwrittenValue", Inheritance.MyConfig.super.overwrittenValue());
        this.inheritedValue = config.requireDouble("inheritedValue");
        this.inheritedDefaultValue = config
            .getNumber("inheritedDefaultValue", Inheritance.MyConfig.super.inheritedDefaultValue())
            .shortValue();
    }

    @Override
    public String baseValue() {
        return this.baseValue;
    }

    @Override
    public int overriddenValue() {
        return this.overriddenValue;
    }

    @Override
    public long overwrittenValue() {
        return this.overwrittenValue;
    }

    @Override
    public double inheritedValue() {
        return this.inheritedValue;
    }

    @Override
    public short inheritedDefaultValue() {
        return this.inheritedDefaultValue;
    }
}
