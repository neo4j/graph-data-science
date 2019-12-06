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
public final class NullableParametersConfig implements NullableParameters {

    private final String referenceTypesDefaultToNotNull;

    private final String referenceTypesCanBeMarkedAsNotNull;

    private final String referenceTypesCanBeMarkedAsNullable;

    private final int extraValue;

    public NullableParametersConfig(
        @NotNull String referenceTypesDefaultToNotNull,
        @NotNull String referenceTypesCanBeMarkedAsNotNull,
        @Nullable String referenceTypesCanBeMarkedAsNullable,
        @NotNull CypherMapWrapper config
    ) {
        this.referenceTypesDefaultToNotNull = CypherMapWrapper.failOnNull(
            "referenceTypesDefaultToNotNull",
            referenceTypesDefaultToNotNull
        );
        this.referenceTypesCanBeMarkedAsNotNull = CypherMapWrapper.failOnNull(
            "referenceTypesCanBeMarkedAsNotNull",
            referenceTypesCanBeMarkedAsNotNull
        );
        this.referenceTypesCanBeMarkedAsNullable = referenceTypesCanBeMarkedAsNullable;
        this.extraValue = config.requireInt("extraValue");
    }

    public NullableParametersConfig(
        @NotNull String referenceTypesDefaultToNotNull,
        @NotNull String referenceTypesCanBeMarkedAsNotNull,
        @NotNull String referenceTypesCanBeMarkedAsNullable,
        int extraValue
    ) {
        this.referenceTypesDefaultToNotNull = CypherMapWrapper.failOnNull(
            "referenceTypesDefaultToNotNull",
            referenceTypesDefaultToNotNull
        );
        this.referenceTypesCanBeMarkedAsNotNull = CypherMapWrapper.failOnNull(
            "referenceTypesCanBeMarkedAsNotNull",
            referenceTypesCanBeMarkedAsNotNull
        );
        this.referenceTypesCanBeMarkedAsNullable = CypherMapWrapper.failOnNull(
            "referenceTypesCanBeMarkedAsNullable",
            referenceTypesCanBeMarkedAsNullable
        );
        this.extraValue = extraValue;
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
}
