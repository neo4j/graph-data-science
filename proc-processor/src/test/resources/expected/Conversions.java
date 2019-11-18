package good;

import org.neo4j.graphalgo.core.CypherMapWrapper;

import javax.annotation.Generated;

@Generated("org.neo4j.graphalgo.proc.ConfigurationProcessor")
public final class ConversionsConfig implements Conversions.MyConversion {

    private final int directMethod;

    private final int inheritedMethod;

    private final int qualifiedMethod;

    public ConversionsConfig(CypherMapWrapper config) {
        this.directMethod = Conversions.MyConversion.toInt(config.requireString("directMethod"));
        this.inheritedMethod = Conversions.BaseConversion.toIntBase(config.requireString("inheritedMethod");
        this.qualifiedMethod = Conversions.OtherConversion.toIntQual(config.requireString("qualifiedMethod");
    }

    @Override
    public int directMethod() {
        return this.directMethod;
    }

    @Override
    public int inheritedMethod() {
        return this.inheritedMethod;
    }

    @Override
    public int qualifiedMethod() {
        return this.qualifiedMethod;
    }
}
