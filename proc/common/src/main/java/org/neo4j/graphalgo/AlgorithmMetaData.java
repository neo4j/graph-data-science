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
package org.neo4j.graphalgo;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.config.BaseConfig;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class AlgorithmMetaData extends AbstractMap<String, Object> implements Map<String, Object> {
    public static final String SIGNATURE_PROPERTY = "gds.procedure.signature";

    private @Nullable BaseConfig config;

    public void set(BaseConfig value) {
        config = value;
    }

    public BaseConfig algoConfig() {
        return Objects.requireNonNull(this.config, "Algo did not set the signature");
    }

    @NotNull
    @Override
    public Set<Entry<String, Object>> entrySet() {
        if (config == null) {
            return Set.of();
        }
        return Set.of(Map.entry(SIGNATURE_PROPERTY, config));
    }
}
