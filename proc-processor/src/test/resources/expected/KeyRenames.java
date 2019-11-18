package good;

import org.neo4j.graphalgo.core.CypherMapWrapper;

import javax.annotation.Generated;

@Generated("org.neo4j.graphalgo.proc.ConfigurationProcessor")
public final class KeyRenamesConfig implements KeyRenames {

    private final int lookupUnderAnotherKey;

    public KeyRenamesConfig(CypherMapWrapper config) {
        this.lookupUnderAnotherKey = config.requireInt("key could also be an invalid identifier");
    }

    @Override
    public int lookupUnderAnotherKey() {
        return this.lookupUnderAnotherKey;
    }
}
