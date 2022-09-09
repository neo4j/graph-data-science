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
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.core.CypherMapWrapper;

@Generated("org.neo4j.gds.proc.ConfigurationProcessor")
public final class NullableParametersConfig implements NullableParameters {
    private String referenceTypesDefaultToNotNull;

    private String referenceTypesCanBeMarkedAsNotNull;

    private String referenceTypesCanBeMarkedAsNullable;

    private int extraValue;

    public NullableParametersConfig(
        @NotNull String referenceTypesDefaultToNotNull,
        @NotNull String referenceTypesCanBeMarkedAsNotNull,
        @Nullable String referenceTypesCanBeMarkedAsNullable,
        @NotNull CypherMapWrapper config
    ) {
        ArrayList<IllegalArgumentException> errors = new ArrayList<>();
        try {
            this.referenceTypesDefaultToNotNull = CypherMapWrapper.failOnNull(
                "referenceTypesDefaultToNotNull",
                referenceTypesDefaultToNotNull
            );
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.referenceTypesCanBeMarkedAsNotNull = CypherMapWrapper.failOnNull(
                "referenceTypesCanBeMarkedAsNotNull",
                referenceTypesCanBeMarkedAsNotNull
            );
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

    public static NullableParametersConfig.Builder builder() {
        return new NullableParametersConfig.Builder();
    }

    public static final class Builder {
        private final Map<String, Object> config;

        private @NotNull String referenceTypesDefaultToNotNull;

        private @NotNull String referenceTypesCanBeMarkedAsNotNull;

        private @Nullable String referenceTypesCanBeMarkedAsNullable;

        public Builder() {
            this.config = new HashMap<>();
        }

        public static NullableParametersConfig.Builder from(NullableParameters baseConfig) {
            var builder = new NullableParametersConfig.Builder();
            builder.referenceTypesDefaultToNotNull(baseConfig.referenceTypesDefaultToNotNull());
            builder.referenceTypesCanBeMarkedAsNotNull(baseConfig.referenceTypesCanBeMarkedAsNotNull());
            builder.referenceTypesCanBeMarkedAsNullable(baseConfig.referenceTypesCanBeMarkedAsNullable());
            builder.extraValue(baseConfig.extraValue());
            return builder;
        }

        public NullableParametersConfig.Builder referenceTypesDefaultToNotNull(
            String referenceTypesDefaultToNotNull) {
            this.referenceTypesDefaultToNotNull = referenceTypesDefaultToNotNull;
            return this;
        }

        public NullableParametersConfig.Builder referenceTypesCanBeMarkedAsNotNull(
            String referenceTypesCanBeMarkedAsNotNull) {
            this.referenceTypesCanBeMarkedAsNotNull = referenceTypesCanBeMarkedAsNotNull;
            return this;
        }

        public NullableParametersConfig.Builder referenceTypesCanBeMarkedAsNullable(
            String referenceTypesCanBeMarkedAsNullable) {
            this.referenceTypesCanBeMarkedAsNullable = referenceTypesCanBeMarkedAsNullable;
            return this;
        }

        public NullableParametersConfig.Builder extraValue(int extraValue) {
            this.config.put("extraValue", extraValue);
            return this;
        }

        public NullableParameters build() {
            CypherMapWrapper config = CypherMapWrapper.create(this.config);
            return new NullableParametersConfig(referenceTypesDefaultToNotNull, referenceTypesCanBeMarkedAsNotNull, referenceTypesCanBeMarkedAsNullable, config);
        }
    }
}
