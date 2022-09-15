/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package positive;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.processing.Generated;

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.core.CypherMapWrapper;

@Generated("org.neo4j.gds.proc.ConfigurationProcessor")
public final class MyConfigImpl implements Inheritance.MyConfig {
    private String baseValue;

    private int overriddenValue;

    private long overwrittenValue;

    private int ignoredInBaseValue;

    private double inheritedValue;

    private short inheritedDefaultValue;

    public MyConfigImpl(@NotNull CypherMapWrapper config) {
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
            this.ignoredInBaseValue = config.requireInt("myKey");
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.inheritedValue = config.requireDouble("inheritedValue");
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.inheritedDefaultValue = config
                .getNumber("inheritedDefaultValue", Inheritance.MyConfig.super.inheritedDefaultValue())
                .shortValue();
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        if (!errors.isEmpty()) {
            if (errors.size() == 1) {
                throw errors.get(0);
            } else {
                String combinedErrorMsg = errors
                    .stream()
                    .map(IllegalArgumentException::getMessage)
                    .collect(Collectors.joining(System.lineSeparator() + "\t\t\t\t",
                        "Multiple errors in configuration arguments:" + System.lineSeparator() + "\t\t\t\t",
                        ""
                    ));
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
    public int ignoredInBaseValue() {
        return this.ignoredInBaseValue;
    }

    @Override
    public double inheritedValue() {
        return this.inheritedValue;
    }

    @Override
    public short inheritedDefaultValue() {
        return this.inheritedDefaultValue;
    }

    public static MyConfigImpl.Builder builder() {
        return new MyConfigImpl.Builder();
    }

    public static final class Builder {
        private final Map<String, Object> config;

        public Builder() {
            this.config = new HashMap<>();
        }

        public static MyConfigImpl.Builder from(Inheritance.MyConfig baseConfig) {
            var builder = new MyConfigImpl.Builder();
            builder.baseValue(baseConfig.baseValue());
            builder.overriddenValue(baseConfig.overriddenValue());
            builder.overwrittenValue(baseConfig.overwrittenValue());
            builder.ignoredInBaseValue(baseConfig.ignoredInBaseValue());
            builder.inheritedValue(baseConfig.inheritedValue());
            builder.inheritedDefaultValue(baseConfig.inheritedDefaultValue());
            return builder;
        }

        public MyConfigImpl.Builder baseValue(String baseValue) {
            this.config.put("baseValue", baseValue);
            return this;
        }

        public MyConfigImpl.Builder overriddenValue(int overriddenValue) {
            this.config.put("overriddenValue", overriddenValue);
            return this;
        }

        public MyConfigImpl.Builder overwrittenValue(long overwrittenValue) {
            this.config.put("overwrittenValue", overwrittenValue);
            return this;
        }

        public MyConfigImpl.Builder ignoredInBaseValue(int ignoredInBaseValue) {
            this.config.put("myKey", ignoredInBaseValue);
            return this;
        }

        public MyConfigImpl.Builder inheritedValue(double inheritedValue) {
            this.config.put("inheritedValue", inheritedValue);
            return this;
        }

        public MyConfigImpl.Builder inheritedDefaultValue(short inheritedDefaultValue) {
            this.config.put("inheritedDefaultValue", inheritedDefaultValue);
            return this;
        }

        public Inheritance.MyConfig build() {
            CypherMapWrapper config = CypherMapWrapper.create(this.config);
            return new MyConfigImpl(config);
        }
    }
}
