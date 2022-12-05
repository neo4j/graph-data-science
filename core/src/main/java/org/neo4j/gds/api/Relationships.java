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
package org.neo4j.gds.api;

import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.annotation.ValueClass;

import java.util.Optional;

@ValueClass
public interface Relationships {

    Topology topology();

    Optional<Properties> properties();

    static Relationships of(
        long relationshipCount,
        boolean isMultiGraph,
        AdjacencyList adjacencyList
    ) {
        return of(
            relationshipCount,
            isMultiGraph,
            adjacencyList,
            null,
            DefaultValue.DOUBLE_DEFAULT_FALLBACK
        );
    }

    static Relationships of(
        long relationshipCount,
        boolean isMultiGraph,
        AdjacencyList adjacencyList,
        @Nullable AdjacencyProperties adjacencyProperties,
        double defaultPropertyValue
    ) {
        var topology = ImmutableTopology.of(
            adjacencyList,
            relationshipCount,
            isMultiGraph
        );

        Optional<Properties> maybePropertyCSR = adjacencyProperties != null
            ? Optional.of(ImmutableProperties.of(
                adjacencyProperties,
                relationshipCount,
                defaultPropertyValue
            )) : Optional.empty();

        return ImmutableRelationships.of(topology, maybePropertyCSR);
    }

    @ValueClass
    interface Topology {
        AdjacencyList adjacencyList();

        @Value.Parameter(false)
        Optional<AdjacencyList> inverseAdjacencyList();

        long elementCount();

        boolean isMultiGraph();

        Topology EMPTY = new Topology() {
            @Override
            public AdjacencyList adjacencyList() {
                return AdjacencyList.EMPTY;
            }

            @Override
            public Optional<AdjacencyList> inverseAdjacencyList() {
                return Optional.empty();
            }

            @Override
            public long elementCount() {
                return 0;
            }

            @Override
            public boolean isMultiGraph() {
                return false;
            }
        };
    }

    @ValueClass
    interface Properties {
        AdjacencyProperties propertiesList();

        long elementCount();

        double defaultPropertyValue();
    }
}
