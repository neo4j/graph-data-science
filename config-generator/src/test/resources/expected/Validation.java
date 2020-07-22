package positive;

import java.util.ArrayList;
import java.util.stream.Collectors;
import javax.annotation.processing.Generated;
import org.jetbrains.annotations.NotNull;
import org.neo4j.graphalgo.core.CypherMapWrapper;

@Generated("org.neo4j.graphalgo.proc.ConfigurationProcessor")
public final class ValidationConfig implements Validation {
    private int foo;

    private ValidationConfig(@NotNull CypherMapWrapper config) {
        ArrayList<IllegalArgumentException> errors = new ArrayList<>();
        try {
            this.foo = config.requireInt("foo");
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            validate();
        } catch (IllegalArgumentException e) {
            errors.add(e);
        } catch (NullPointerException e) {
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

    public static Validation of(@NotNull CypherMapWrapper config) {
        Validation instance = new ValidationConfig(config);
        instance = instance.normalize();
        return instance;
    }

    @Override
    public int foo() {
        return this.foo;
    }
}