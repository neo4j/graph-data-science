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
package org.neo4j.graphalgo.config;

import org.immutables.value.Value;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.graphalgo.api.GraphStoreFactory;

@ValueClass
@Configuration
@SuppressWarnings("immutables:subtype")
public interface GraphCreateFromGraphConfig extends GraphCreateConfig {

    @Configuration.Parameter
    String graphName();

    @Configuration.Parameter
    String fromGraphName();

    @Configuration.Parameter
    String nodeFilter();

    @Configuration.Parameter
    String relationshipFilter();

    @Configuration.Parameter
    GraphCreateConfig originalConfig();

    @Value.Default
    @Value.Parameter(false)
    default int concurrency() {
        return ConcurrencyConfig.DEFAULT_CONCURRENCY;
    }

    @Value.Check
    default void validateReadConcurrency() {
        ConcurrencyConfig.validateConcurrency(concurrency(), "concurrency");
    }

    @Value.Default
    @Configuration.Ignore
    @Override
    default GraphStoreFactory.Supplier graphStoreFactory() {
        return originalConfig().graphStoreFactory();
    }

    @Override
    @Configuration.Ignore
    default <R> R accept(Cases<R> visitor) {
        return visitor.graph(this);
    }

    // Inherited, but ignored config keys

    @Override
    @Configuration.Ignore
    default long nodeCount() {
        return -1;
    }

    @Override
    @Configuration.Ignore
    default long relationshipCount() {
        return -1;
    }

    @Override
    @Configuration.Ignore
    default boolean validateRelationships() {
        return false;
    }

    static GraphCreateFromGraphConfig of(
        String userName,
        String graphName,
        String fromGraphName,
        String nodeFilter,
        String relationshipFilter,
        GraphCreateConfig originalConfig,
        CypherMapWrapper procedureConfig
    ) {
        return new GraphCreateFromGraphConfigImpl(
            graphName,
            fromGraphName,
            nodeFilter,
            relationshipFilter,
            originalConfig,
            userName,
            procedureConfig
        );
    }
}
