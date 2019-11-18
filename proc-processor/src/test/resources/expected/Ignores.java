package good;

import org.neo4j.graphalgo.core.CypherMapWrapper;

import javax.annotation.Generated;

@Generated("org.neo4j.graphalgo.proc.ConfigurationProcessor")
public final class MyConfig implements Ignores.MyConfig {

    private final long notIgnored;

    public MyConfig(CypherMapWrapper config) {
        this.notIgnored = config.requireLong("notIgnored");
    }

    @Override
    public long notIgnored() {
        return this.notIgnored;
    }
}
