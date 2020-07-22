package positive;

import java.util.ArrayList;
import java.util.stream.Collectors;
import javax.annotation.processing.Generated;
import org.jetbrains.annotations.NotNull;
import org.neo4j.graphalgo.core.CypherMapWrapper;

@Generated("org.neo4j.graphalgo.proc.ConfigurationProcessor")
public final class ParametersConfig implements Parameters {
    private int keyFromParameter;

    private long keyFromMap;

    private int parametersAreAddedFirst;

    public ParametersConfig(int keyFromParameter, int parametersAreAddedFirst,
                            @NotNull CypherMapWrapper config) {
        ArrayList<IllegalArgumentException> errors = new ArrayList<>();
        try {
            this.keyFromParameter = keyFromParameter;
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.keyFromMap = config.requireLong("keyFromMap");
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.parametersAreAddedFirst = parametersAreAddedFirst;
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
    public int keyFromParameter() {
        return this.keyFromParameter;
    }

    @Override
    public long keyFromMap() {
        return this.keyFromMap;
    }

    @Override
    public int parametersAreAddedFirst() {
        return this.parametersAreAddedFirst;
    }
}