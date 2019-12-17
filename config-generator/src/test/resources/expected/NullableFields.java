/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.core.CypherMapWrapper;

import javax.annotation.processing.Generated;

@Generated("org.neo4j.graphalgo.proc.ConfigurationProcessor")
public final class NullableFieldsConfig implements NullableFields {

    private final @Nullable String nullableRequiredField;

    private final @Nullable String nullableDefaultField;

    private final @Nullable String conversionCanReturnNull;

    public NullableFieldsConfig(@NotNull CypherMapWrapper config) {
        this.nullableRequiredField = config.requireString("nullableRequiredField");
        this.nullableDefaultField = config.getString("nullableDefaultField", NullableFields.super.nullableDefaultField());
        this.conversionCanReturnNull = NullableFields.emptyToNull(config.getString("conversionCanReturnNull", NullableFields.super.conversionCanReturnNull()));
    }

    public NullableParametersConfig(
        @NotNull String nullableRequiredField,
        @NotNull String nullableDefaultField,
        @NotNull String conversionCanReturnNull
    ) {
        this.nullableRequiredField = CypherMapWrapper.failOnNull("nullableRequiredField", nullableRequiredField);
        this.nullableDefaultField = CypherMapWrapper.failOnNull("nullableDefaultField", nullableDefaultField);
        this.conversionCanReturnNull = CypherMapWrapper.failOnNull("conversionCanReturnNull", conversionCanReturnNull);
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
