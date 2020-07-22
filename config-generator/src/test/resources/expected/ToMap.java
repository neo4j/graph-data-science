package positive;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.processing.Generated;
import org.jetbrains.annotations.NotNull;
import org.neo4j.graphalgo.core.CypherMapWrapper;

@Generated("org.neo4j.graphalgo.proc.ConfigurationProcessor")
public final class ToMapConfig implements ToMap {
    private int foo;

    private long bar;

    private double baz;

    private Optional<Long> maybeBar;

    private Optional<Double> maybeBaz;

    public ToMapConfig(int foo, @NotNull CypherMapWrapper config) {
        ArrayList<IllegalArgumentException> errors = new ArrayList<>();
        try {
            this.foo = foo;
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.bar = config.requireLong("bar");
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.baz = config.requireDouble("baz");
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.maybeBar = CypherMapWrapper.failOnNull("maybeBar", config.getOptional("maybeBar", Long.class));
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.maybeBaz = CypherMapWrapper.failOnNull("maybeBaz", config.getOptional("maybeBaz", Double.class));
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
    public int foo() {
        return this.foo;
    }

    @Override
    public long bar() {
        return this.bar;
    }

    @Override
    public double baz() {
        return this.baz;
    }

    @Override
    public Optional<Long> maybeBar() {
        return this.maybeBar;
    }

    @Override
    public Optional<Double> maybeBaz() {
        return this.maybeBaz;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("bar", bar());
        map.put("baz", positive.ToMap.add42(baz()));
        maybeBar().ifPresent(maybeBar -> map.put("maybeBar", maybeBar));
        maybeBaz().ifPresent(maybeBaz -> map.put("maybeBaz", positive.ToMap.add42(maybeBaz)));
        return map;
    }
}