package good;

import org.neo4j.graphalgo.core.CypherMapWrapper;

import javax.annotation.Generated;

@Generated("org.neo4j.graphalgo.proc.ConfigurationProcessor")
public final class NamingConflictConfig implements NamingConflict {

    private final int config;

    public KeyRenamesConfig(CypherMapWrapper config_) {
        this.config = config_.requireInt("config");
    }

    @Override
    public int config() {
        return this.config;
    }
}
