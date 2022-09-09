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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.processing.Generated;
import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.core.CypherMapWrapper;

@Generated("org.neo4j.gds.proc.ConfigurationProcessor")
public final class RangeValidationConfig implements RangeValidation {
    private int integerWithinRange;

    private long longWithinRange;

    private double doubleWithinRange;

    private Optional<Double> maybeDoubleWithinRange;

    private List<Double> listDoubleWithinRange;

    public RangeValidationConfig(@NotNull CypherMapWrapper config) {
        ArrayList<IllegalArgumentException> errors = new ArrayList<>();
        try {
            this.integerWithinRange = config.requireInt("integerWithinRange")
            CypherMapWrapper.validateIntegerRange(
                "integerWithinRange",
                integerWithinRange,
                21,
                42,
                false,
                true
            );
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.longWithinRange = config.requireLong("longWithinRange")
            CypherMapWrapper.validateLongRange(
                "longWithinRange",
                longWithinRange,
                21L,
                42L,
                false,
                true);
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.doubleWithinRange = config.requireDouble("doubleWithinRange");
            CypherMapWrapper.validateDoubleRange(
                "doubleWithinRange",
                doubleWithinRange,
                21.0,
                42.0,
                false,
                true
            );
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.maybeDoubleWithinRange = CypherMapWrapper.failOnNull(
                "maybeDoubleWithinRange",
                config.getOptional("maybeDoubleWithinRange", Double.class)
            );
            maybeDoubleWithinRange.ifPresent(maybeDoubleWithinRange ->
                CypherMapWrapper.validateDoubleRange(
                    "maybeDoubleWithinRange",
                    maybeDoubleWithinRange,
                    21.0,
                    42.0,
                    false,
                    true
                )
            );
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.listDoubleWithinRange = CypherMapWrapper.failOnNull(
                "listDoubleWithinRange",
                config.requireChecked("listDoubleWithinRange", List.class)
            );
            this.listDoubleWithinRange.forEach(listDoubleWithinRange -> CypherMapWrapper.validateDoubleRange(
                "listDoubleWithinRange",
                listDoubleWithinRange,
                21.0,
                42.0,
                false,
                true
            ));
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
                    .collect(Collectors.joining(
                        System.lineSeparator() + "\t\t\t\t",
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
    public int integerWithinRange() {
        return this.integerWithinRange;
    }

    @Override
    public long longWithinRange() {
        return this.longWithinRange;
    }

    @Override
    public double doubleWithinRange() {
        return this.doubleWithinRange;
    }

    @Override
    public Optional<Double> maybeDoubleWithinRange() {
        return this.maybeDoubleWithinRange;
    }

    @Override
    public List<Double> listDoubleWithinRange() {
        return this.listDoubleWithinRange;
    }

    public static RangeValidationConfig.Builder builder() {
        return new RangeValidationConfig.Builder();
    }

    public static final class Builder {
        private final Map<String, Object> config;

        public Builder() {
            this.config = new HashMap<>();
        }

        public static RangeValidationConfig.Builder from(RangeValidation baseConfig) {
            var builder = new RangeValidationConfig.Builder();
            builder.integerWithinRange(baseConfig.integerWithinRange());
            builder.longWithinRange(baseConfig.longWithinRange());
            builder.doubleWithinRange(baseConfig.doubleWithinRange());
            builder.maybeDoubleWithinRange(baseConfig.maybeDoubleWithinRange());
            builder.listDoubleWithinRange(baseConfig.listDoubleWithinRange());
            return builder;
        }

        public RangeValidationConfig.Builder integerWithinRange(int integerWithinRange) {
            this.config.put("integerWithinRange", integerWithinRange);
            return this;
        }

        public RangeValidationConfig.Builder longWithinRange(long longWithinRange) {
            this.config.put("longWithinRange", longWithinRange);
            return this;
        }

        public RangeValidationConfig.Builder doubleWithinRange(double doubleWithinRange) {
            this.config.put("doubleWithinRange", doubleWithinRange);
            return this;
        }

        public RangeValidationConfig.Builder maybeDoubleWithinRange(Double maybeDoubleWithinRange) {
            this.config.put("maybeDoubleWithinRange", maybeDoubleWithinRange);
            return this;
        }

        public RangeValidationConfig.Builder maybeDoubleWithinRange(
            Optional<Double> maybeDoubleWithinRange) {
            maybeDoubleWithinRange.ifPresent(actualmaybeDoubleWithinRange -> this.config.put("maybeDoubleWithinRange", actualmaybeDoubleWithinRange));
            return this;
        }

        public RangeValidationConfig.Builder listDoubleWithinRange(
            List<Double> listDoubleWithinRange) {
            this.config.put("listDoubleWithinRange", listDoubleWithinRange);
            return this;
        }

        public RangeValidation build() {
            CypherMapWrapper config = CypherMapWrapper.create(this.config);
            return new RangeValidationConfig(config);
        }
    }
}
