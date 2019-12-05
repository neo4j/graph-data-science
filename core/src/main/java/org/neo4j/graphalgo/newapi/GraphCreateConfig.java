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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;
import org.neo4j.graphalgo.NodeProjections;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.RelationshipProjections;
import org.neo4j.graphalgo.annotation.Configuration;
import org.neo4j.graphalgo.annotation.Configuration.ConvertWith;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.ProcedureConstants;
import org.neo4j.graphalgo.core.utils.Pools;

@ValueClass
@Configuration("GraphCreateConfigImpl")
public interface GraphCreateConfig extends BaseConfig {

    @NotNull String IMPLICIT_GRAPH_NAME = "";

    @Configuration.Parameter
    String graphName();

    @Configuration.Parameter
    @ConvertWith("org.neo4j.graphalgo.NodeProjections#fromObject")
    NodeProjections nodeProjection();

    @Configuration.Parameter
    @ConvertWith("org.neo4j.graphalgo.AbstractRelationshipProjections#fromObject")
    RelationshipProjections relationshipProjection();

    @Value.Default
    @Value.Parameter(false)
    @ConvertWith("org.neo4j.graphalgo.AbstractPropertyMappings#fromObject")
    default PropertyMappings nodeProperties() {
        return PropertyMappings.of();
    }

    @Value.Default
    @Value.Parameter(false)
    @ConvertWith("org.neo4j.graphalgo.AbstractPropertyMappings#fromObject")
    default PropertyMappings relationshipProperties() {
        return PropertyMappings.of();
    }

    @Value.Default
    @Value.Parameter(false)
    @Configuration.Key(ProcedureConstants.READ_CONCURRENCY_KEY)
    default int concurrency() {
        return Pools.DEFAULT_CONCURRENCY;
    }

    @Value.Check
    @Configuration.Ignore
    default GraphCreateConfig withNormalizedPropertyMappings() {
        PropertyMappings nodeProperties = nodeProperties();
        PropertyMappings relationshipProperties = relationshipProperties();
        if (nodeProperties.hasMappings() || relationshipProperties.hasMappings()) {
            return ImmutableGraphCreateConfig
                .builder()
                .from(this)
                .nodeProjection(nodeProjection().addPropertyMappings(nodeProperties))
                .nodeProperties(PropertyMappings.of())
                .relationshipProjection(relationshipProjection().addPropertyMappings(relationshipProperties))
                .relationshipProperties(PropertyMappings.of())
                .build();
        }
        return this;
    }

    static GraphCreateConfig legacyFactory(String graphName) {
        return ImmutableGraphCreateConfig
            .builder()
            .graphName(graphName)
            .nodeProjection(NodeProjections.empty())
            .relationshipProjection(RelationshipProjections.empty())
            .concurrency(-1)
            .build();
    }

    @TestOnly
    static GraphCreateConfig emptyWithName(String userName, String name) {
        return ImmutableGraphCreateConfig.of(userName, name, NodeProjections.empty(), RelationshipProjections.empty());
    }

    static GraphCreateConfig of(
        String userName,
        String graphName,
        @Nullable Object nodeFilter,
        @Nullable Object relationshipFilter,
        CypherMapWrapper config
    ) {
        GraphCreateConfig graphCreateConfig = new GraphCreateConfigImpl(
            graphName,
            nodeFilter,
            relationshipFilter,
            userName,
            config
        );
        return graphCreateConfig.withNormalizedPropertyMappings();
    }

    static GraphCreateConfig implicitCreate(
        String userName,
        CypherMapWrapper config
    ) {
        RelationshipProjections relP = RelationshipProjections.fromObject(CypherMapWrapper.failOnNull(
            "relationshipProjection",
            config.get("relationshipProjection", RelationshipProjections.empty())
        ));
        NodeProjections nodeP = NodeProjections.fromObject(CypherMapWrapper.failOnNull(
            "nodeProjection",
            config.get("nodeProjection", NodeProjections.empty())
        ));
        GraphCreateConfig graphCreateConfig = new GraphCreateConfigImpl(
            IMPLICIT_GRAPH_NAME,
            nodeP,
            relP,
            userName,
            config
        );
        return graphCreateConfig.withNormalizedPropertyMappings();
    }
}
