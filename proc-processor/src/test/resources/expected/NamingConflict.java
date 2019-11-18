package good;

import org.neo4j.graphalgo.core.CypherMapWrapper;

import javax.annotation.Generated;

@Generated("org.neo4j.graphalgo.proc.ConfigurationProcessor")
public final class NamingConflictConfig implements NamingConflict {

    private final int config;

    private final int anotherConfig;

    private final int config_;

    public KeyRenamesConfig(int config_, CypherMapWrapper config__) {
        this.config = config__.requireInt("config");
        this.anotherConfig = config__.requireInt("config");
        this.config_ = config_;
    }

    @Override
    public int config() {
        return this.config;
    }

    @Override
    public int anotherConfig() {
        return this.anotherConfig;
    }

    @Override
    public int config_() {
        return this.config_;
    }
}
