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

package org.neo4j.graphalgo.newapi;

import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;
import org.neo4j.graphalgo.NodeFilters;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.RelationshipFilters;
import org.neo4j.graphalgo.annotation.Configuration;
import org.neo4j.graphalgo.annotation.Configuration.ConvertWith;
import org.neo4j.graphalgo.annotation.Configuration.Parameter;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConstants;
import org.neo4j.graphalgo.core.utils.Pools;

@ValueClass
@Configuration("GraphCreateConfigImpl")
public interface GraphCreateConfig extends BaseConfig {

    @Parameter
    String graphName();

    @Parameter(acceptNull = true)
    @ConvertWith("org.neo4j.graphalgo.NodeFilters#fromObject")
    default NodeFilters nodeFilter() {
        return NodeFilters.empty();
    }

    @Parameter(acceptNull = true)
    @ConvertWith("org.neo4j.graphalgo.RelationshipFilters#fromObject")
    default RelationshipFilters relationshipFilter() {
        return RelationshipFilters.empty();
    }

    @ConvertWith("org.neo4j.graphalgo.PropertyMappings#fromObject")
    default PropertyMappings nodeProperties() {
        return PropertyMappings.EMPTY;
    }

    @ConvertWith("org.neo4j.graphalgo.PropertyMappings#fromObject")
    default PropertyMappings relationshipProperties() {
        return PropertyMappings.EMPTY;
    }

    @Value.Default
    @Value.Parameter(false)
    @Configuration.Key(ProcedureConstants.READ_CONCURRENCY_KEY)
    default int concurrency() {
        return Pools.DEFAULT_CONCURRENCY;
    }

    @Override
    @Configuration.Ignore
    default GraphLoader configureLoader(GraphLoader loader) {
        return loader
            .withName(graphName())
            .withOptionalLabel(nodeFilter().labelFilter().orElse(null))
            .withOptionalRelationshipType(relationshipFilter().typeFilter())
            .withConcurrency(concurrency())
            .withLoadedGraph(true);
    }

    static GraphCreateConfig legacyFactory(String graphName) {
        return ImmutableGraphCreateConfig
            .builder()
            .graphName(graphName)
            .concurrency(-1)
            .build();
    }

    @TestOnly
    static GraphCreateConfig emptyWithName(String userName, String name) {
        return ImmutableGraphCreateConfig.of(userName, name);
    }

    static GraphCreateConfig of(
        String userName,
        String graphName,
        @Nullable Object nodeFilter,
        @Nullable Object relationshipFilter,
        CypherMapWrapper config
    ) {
        return new GraphCreateConfigImpl(
            graphName,
            nodeFilter,
            relationshipFilter,
            userName,
            config
        );
    }
}
