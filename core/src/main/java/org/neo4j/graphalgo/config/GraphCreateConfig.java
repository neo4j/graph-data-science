/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
import org.neo4j.graphalgo.NodeProjections;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.RelationshipProjections;
import org.neo4j.graphalgo.annotation.Configuration;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.ProcedureConstants;

import static org.neo4j.graphalgo.config.GraphCreateFromCypherConfig.NODE_QUERY_KEY;
import static org.neo4j.graphalgo.config.GraphCreateFromCypherConfig.RELATIONSHIP_QUERY_KEY;

public interface GraphCreateConfig extends BaseConfig {

    String IMPLICIT_GRAPH_NAME = "";

    String NODE_PROJECTIONS_KEY = "nodeProjection";
    String RELATIONSHIP_PROJECTIONS_KEY = "relationshipProjection";

    @Configuration.Parameter
    String graphName();

    @Configuration.Key(NODE_PROJECTIONS_KEY)
    NodeProjections nodeProjections();

    @Configuration.Key(RELATIONSHIP_PROJECTIONS_KEY)
    RelationshipProjections relationshipProjections();

    @Value.Default
    @Value.Parameter(false)
    @Configuration.ConvertWith("org.neo4j.graphalgo.AbstractPropertyMappings#fromObject")
    default PropertyMappings nodeProperties() {
        return PropertyMappings.of();
    }

    @Value.Default
    @Value.Parameter(false)
    @Configuration.ConvertWith("org.neo4j.graphalgo.AbstractPropertyMappings#fromObject")
    default PropertyMappings relationshipProperties() {
        return PropertyMappings.of();
    }

    @Value.Default
    @Value.Parameter(false)
    default int readConcurrency() {
        return AlgoBaseConfig.DEFAULT_CONCURRENCY;
    }

    @Value.Default
    @Value.Parameter(false)
    @Configuration.Key(ProcedureConstants.NODECOUNT_KEY)
    default long nodeCount() {
        return -1;
    }

    @Value.Default
    @Value.Parameter(false)
    @Configuration.Key(ProcedureConstants.RELCOUNT_KEY)
    default long relationshipCount() {
        return -1;
    }

    static GraphCreateConfig createImplicit(String username, CypherMapWrapper config) {
        if (config.containsKey(NODE_QUERY_KEY) && config.containsKey(RELATIONSHIP_QUERY_KEY)) {
            return GraphCreateFromCypherConfig.fromProcedureConfig(username, config);
        } else if (config.containsKey(NODE_PROJECTIONS_KEY) && config.containsKey(RELATIONSHIP_PROJECTIONS_KEY)) {
            return GraphCreateFromStoreConfig.fromProcedureConfig(username, config);
        } else {
            throw new IllegalArgumentException("Missing information for implicit graph creation: node / relationship projections or queries.");
        }
    }
}
