package positive;

import java.util.ArrayList;
import java.util.stream.Collectors;
import javax.annotation.processing.Generated;
import org.jetbrains.annotations.NotNull;
import org.neo4j.graphalgo.core.CypherMapWrapper;

@Generated("org.neo4j.graphalgo.proc.ConfigurationProcessor")
public final class MyConfig implements Inheritance.MyConfig {
    private String baseValue;

    private int overriddenValue;

    private long overwrittenValue;

    private double inheritedValue;

    private short inheritedDefaultValue;

    public MyConfig(@NotNull CypherMapWrapper config) {
        ArrayList<IllegalArgumentException> errors = new ArrayList<>();
        try {
            this.baseValue = CypherMapWrapper.failOnNull("baseValue", config.requireString("baseValue"));
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.overriddenValue = config.getInt("overriddenValue", Inheritance.MyConfig.super.overriddenValue());
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.overwrittenValue = config.getLong("overwrittenValue", Inheritance.MyConfig.super.overwrittenValue());
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.inheritedValue = config.requireDouble("inheritedValue");
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.inheritedDefaultValue = config.getNumber("inheritedDefaultValue", Inheritance.MyConfig.super.inheritedDefaultValue()).shortValue();
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
    public String baseValue() {
        return this.baseValue;
    }

    @Override
    public int overriddenValue() {
        return this.overriddenValue;
    }

    @Override
    public long overwrittenValue() {
        return this.overwrittenValue;
    }

    @Override
    public double inheritedValue() {
        return this.inheritedValue;
    }

    @Override
    public short inheritedDefaultValue() {
        return this.inheritedDefaultValue;
    }
}