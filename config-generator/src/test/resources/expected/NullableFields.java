package positive;

import java.util.ArrayList;
import java.util.stream.Collectors;
import javax.annotation.processing.Generated;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.core.CypherMapWrapper;

@Generated("org.neo4j.graphalgo.proc.ConfigurationProcessor")
public final class NullableFieldsConfig implements NullableFields {
    private @Nullable String nullableRequiredField;

    private @Nullable String nullableDefaultField;

    private @Nullable String conversionCanReturnNull;

    public NullableFieldsConfig(@NotNull CypherMapWrapper config) {
        ArrayList<IllegalArgumentException> errors = new ArrayList<>();
        try {
            this.nullableRequiredField = config.requireString("nullableRequiredField");
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.nullableDefaultField = config.getString("nullableDefaultField", NullableFields.super.nullableDefaultField());
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.conversionCanReturnNull = NullableFields.emptyToNull(config.getString("conversionCanReturnNull", NullableFields.super.conversionCanReturnNull()));
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
    public @Nullable String nullableRequiredField() {
        return this.nullableRequiredField;
    }

    @Override
    public @Nullable String nullableDefaultField() {
        return this.nullableDefaultField;
    }

    @Override
    public @Nullable String conversionCanReturnNull() {
        return this.conversionCanReturnNull;
    }
}