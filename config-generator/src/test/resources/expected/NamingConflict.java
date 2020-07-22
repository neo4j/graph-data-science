package positive;

import java.util.ArrayList;
import java.util.stream.Collectors;
import javax.annotation.processing.Generated;
import org.jetbrains.annotations.NotNull;
import org.neo4j.graphalgo.core.CypherMapWrapper;

@Generated("org.neo4j.graphalgo.proc.ConfigurationProcessor")
public final class NamingConflictConfig implements NamingConflict {
    private int config;

    private int anotherConfig;

    private int config_;

    public NamingConflictConfig(int config_, @NotNull CypherMapWrapper config__) {
        ArrayList<IllegalArgumentException> errors = new ArrayList<>();
        try {
            this.config = config__.requireInt("config");
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.anotherConfig = config__.requireInt("config");
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.config_ = config_;
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        if(!errors.isEmpty()) {
            if(errors.size() == 1) {
                throw errors.get(0);
            } else {
                String combinedErrorMsg = errors.stream().map(IllegalArgumentException::getMessage).collect(Collectors.joining(System.lineSeparator() + "\t\t\t\t", "Multiple errors in configuration arguments:" + System.lineSeparator() + "\t\t\t\t", ""));
                IllegalArgumentException combinedError = new IllegalArgumentException(combinedErrorMsg);
                errors.forEach(error -> combinedError.addSuppressed(error));
                throw combinedError;
            }
        }
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