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

    String KEY_NODE_PROJECTIONS = "nodeProjection";
    String KEY_RELATIONSHIP_PROJECTIONS = "relationshipProjection";

    @Configuration.Parameter
    String graphName();

    @Configuration.Key(KEY_NODE_PROJECTIONS)
    NodeProjections nodeProjections();

    @Configuration.Key(KEY_RELATIONSHIP_PROJECTIONS)
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
    @Configuration.Key(ProcedureConstants.READ_CONCURRENCY_KEY)
    default int concurrency() {
        return AlgoBaseConfig.DEFAULT_CONCURRENCY;
    }

    @Value.Default
    @Value.Parameter(false)
    @Configuration.Key(ProcedureConstants.NODECOUNT_KEY)
    default int nodeCount() {
        return -1;
    }

    @Value.Default
    @Value.Parameter(false)
    @Configuration.Key(ProcedureConstants.RELCOUNT_KEY)
    default int relationshipCount() {
        return -1;
    }


    static GraphCreateConfig createImplicit(String username, CypherMapWrapper config) {
        if (config.containsKey(NODE_QUERY_KEY) || config.containsKey(RELATIONSHIP_QUERY_KEY)) {
            return GraphCreateFromCypherConfig.fromProcedureConfig(username, config);
        } else {
            return GraphCreateFromStoreConfig.fromProcedureConfig(username, config);
        }
    }
}
