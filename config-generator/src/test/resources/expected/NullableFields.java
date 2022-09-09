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
            this.nullableDefaultField = config.getString(
                "nullableDefaultField",
                NullableFields.super.nullableDefaultField()
            );
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.conversionCanReturnNull = NullableFields.emptyToNull(config.getString(
                "conversionCanReturnNull",
                NullableFields.super.conversionCanReturnNull()
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

    public static NullableFieldsConfig.Builder builder() {
        return new NullableFieldsConfig.Builder();
    }

    public static final class Builder {
        private final Map<String, Object> config;

        public Builder() {
            this.config = new HashMap<>();
        }

        public static NullableFieldsConfig.Builder from(NullableFields baseConfig) {
            var builder = new NullableFieldsConfig.Builder();
            builder.nullableRequiredField(baseConfig.nullableRequiredField());
            builder.nullableDefaultField(baseConfig.nullableDefaultField());
            builder.conversionCanReturnNull(positive.NullableFields.toInputString(baseConfig.conversionCanReturnNull()));
            return builder;
        }

        public NullableFieldsConfig.Builder nullableRequiredField(String nullableRequiredField) {
            this.config.put("nullableRequiredField", nullableRequiredField);
            return this;
        }

        public NullableFieldsConfig.Builder nullableDefaultField(String nullableDefaultField) {
            this.config.put("nullableDefaultField", nullableDefaultField);
            return this;
        }

        public NullableFieldsConfig.Builder conversionCanReturnNull(
            String conversionCanReturnNull) {
            this.config.put("conversionCanReturnNull", conversionCanReturnNull);
            return this;
        }

        public NullableFields build() {
            CypherMapWrapper config = CypherMapWrapper.create(this.config);
            return new NullableFieldsConfig(config);
        }
    }
}
