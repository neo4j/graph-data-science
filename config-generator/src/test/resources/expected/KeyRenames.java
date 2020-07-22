package positive;

import java.util.ArrayList;
import java.util.stream.Collectors;
import javax.annotation.processing.Generated;
import org.jetbrains.annotations.NotNull;
import org.neo4j.graphalgo.core.CypherMapWrapper;

@Generated("org.neo4j.graphalgo.proc.ConfigurationProcessor")
public final class KeyRenamesConfig implements KeyRenames {
    private int lookupUnderAnotherKey;

    private int whitespaceWillBeTrimmed;

    public KeyRenamesConfig(@NotNull CypherMapWrapper config) {
        ArrayList<IllegalArgumentException> errors = new ArrayList<>();
        try {
            this.lookupUnderAnotherKey = config.requireInt("key could also be an invalid identifier");
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.whitespaceWillBeTrimmed = config.requireInt("whitespace will be trimmed");
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
    public int lookupUnderAnotherKey() {
        return this.lookupUnderAnotherKey;
    }

    @Override
    public int whitespaceWillBeTrimmed() {
        return this.whitespaceWillBeTrimmed;
    }
}