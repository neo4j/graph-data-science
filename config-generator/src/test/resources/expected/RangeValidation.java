package positive;

import java.util.ArrayList;
import java.util.stream.Collectors;
import javax.annotation.processing.Generated;
import org.jetbrains.annotations.NotNull;
import org.neo4j.graphalgo.core.CypherMapWrapper;

@Generated("org.neo4j.graphalgo.proc.ConfigurationProcessor")
public final class RangeValidationConfig implements RangeValidation {
    private int integerWithinRange;

    private double doubleWithinRange;

    public RangeValidationConfig(@NotNull CypherMapWrapper config) {
        ArrayList<IllegalArgumentException> errors = new ArrayList<>();
        try {
            this.integerWithinRange = CypherMapWrapper.validateIntegerRange("integerWithinRange", config.requireInt("integerWithinRange"), 21, 42, false, true);
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.doubleWithinRange = CypherMapWrapper.validateDoubleRange("doubleWithinRange", config.requireDouble("doubleWithinRange"), 21.0, 42.0, false, true);
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
    public int integerWithinRange() {
        return this.integerWithinRange;
    }

    @Override
    public double doubleWithinRange() {
        return this.doubleWithinRange;
    }
}