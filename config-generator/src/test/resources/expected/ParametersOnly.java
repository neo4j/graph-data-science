package positive;

import java.util.ArrayList;
import java.util.stream.Collectors;
import javax.annotation.processing.Generated;

@Generated("org.neo4j.graphalgo.proc.ConfigurationProcessor")
public final class ParametersOnlyConfig implements ParametersOnly {
    private int onlyAsParameter;

    public ParametersOnlyConfig(int onlyAsParameter) {
        ArrayList<IllegalArgumentException> errors = new ArrayList<>();
        try {
            this.onlyAsParameter = onlyAsParameter;
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
    public int onlyAsParameter() {
        return this.onlyAsParameter;
    }
}