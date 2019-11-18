package good;

import org.neo4j.graphalgo.core.CypherMapWrapper;

import javax.annotation.Generated;

@Generated("org.neo4j.graphalgo.proc.ConfigurationProcessor")
public final class KeyRenamesConfig implements KeyRenames {

    private final int lookupUnderAnotherKey;

    private final int whitespaceWillBeTrimmed;

    public KeyRenamesConfig(CypherMapWrapper config) {
        this.lookupUnderAnotherKey = config.requireInt("key could also be an invalid identifier");
        this.whitespaceWillBeTrimmed = config.requireInt("whitespace will be trimmed");
    }

    @Override
    public int lookupUnderAnotherKey() {
        return this.lookupUnderAnotherKey;
    }

    @Override
    public int whitespaceWillBeTrimmed() {
        return this.whitespaceWillBeTrimmed;
    }
}
