package positive;

import java.util.ArrayList;
import java.util.stream.Collectors;
import javax.annotation.processing.Generated;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.core.CypherMapWrapper;

@Generated("org.neo4j.graphalgo.proc.ConfigurationProcessor")
public final class NullableParametersConfig implements NullableParameters {
    private String referenceTypesDefaultToNotNull;

    private String referenceTypesCanBeMarkedAsNotNull;

    private String referenceTypesCanBeMarkedAsNullable;

    private int extraValue;

    public NullableParametersConfig(@NotNull String referenceTypesDefaultToNotNull,
                                    @NotNull String referenceTypesCanBeMarkedAsNotNull,
                                    @Nullable String referenceTypesCanBeMarkedAsNullable,
                                    @NotNull CypherMapWrapper config) {
        ArrayList<IllegalArgumentException> errors = new ArrayList<>();
        try {
            this.referenceTypesDefaultToNotNull = CypherMapWrapper.failOnNull("referenceTypesDefaultToNotNull", referenceTypesDefaultToNotNull);
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.referenceTypesCanBeMarkedAsNotNull = CypherMapWrapper.failOnNull("referenceTypesCanBeMarkedAsNotNull", referenceTypesCanBeMarkedAsNotNull);
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.referenceTypesCanBeMarkedAsNullable = referenceTypesCanBeMarkedAsNullable;
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.extraValue = config.requireInt("extraValue");
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
    public String referenceTypesDefaultToNotNull() {
        return this.referenceTypesDefaultToNotNull;
    }

    @Override
    public String referenceTypesCanBeMarkedAsNotNull() {
        return this.referenceTypesCanBeMarkedAsNotNull;
    }

    @Override
    public String referenceTypesCanBeMarkedAsNullable() {
        return this.referenceTypesCanBeMarkedAsNullable;
    }

    @Override
    public int extraValue() {
        return this.extraValue;
    }
}