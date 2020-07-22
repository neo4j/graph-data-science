package positive;

import java.util.ArrayList;
import java.util.stream.Collectors;
import javax.annotation.processing.Generated;
import org.jetbrains.annotations.NotNull;
import org.neo4j.graphalgo.core.CypherMapWrapper;

@Generated("org.neo4j.graphalgo.proc.ConfigurationProcessor")
public final class DefaultValuesConfig implements DefaultValues {
    private int defaultInt;

    private String defaultString;

    public DefaultValuesConfig(@NotNull CypherMapWrapper config) {
        ArrayList<IllegalArgumentException> errors = new ArrayList<>();
        try {
            this.defaultInt = config.getInt("defaultInt", DefaultValues.super.defaultInt());
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.defaultString = CypherMapWrapper.failOnNull("defaultString", config.getString("defaultString", DefaultValues.super.defaultString()));
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
    public int defaultInt() {
        return this.defaultInt;
    }

    @Override
    public String defaultString() {
        return this.defaultString;
    }
}