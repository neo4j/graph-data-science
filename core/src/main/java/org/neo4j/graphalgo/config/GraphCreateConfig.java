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
import org.neo4j.graphalgo.annotation.Configuration;
import org.neo4j.graphalgo.api.GraphStoreFactory;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.loading.CypherFactory;
import org.neo4j.graphalgo.core.loading.NativeFactory;
import org.neo4j.graphalgo.core.utils.TimeUtil;

import java.time.ZonedDateTime;

import static org.neo4j.graphalgo.config.GraphCreateFromCypherConfig.NODE_QUERY_KEY;
import static org.neo4j.graphalgo.config.GraphCreateFromCypherConfig.RELATIONSHIP_QUERY_KEY;
import static org.neo4j.graphalgo.config.GraphCreateFromStoreConfig.NODE_PROJECTION_KEY;
import static org.neo4j.graphalgo.config.GraphCreateFromStoreConfig.RELATIONSHIP_PROJECTION_KEY;

public interface GraphCreateConfig extends BaseConfig {

    String IMPLICIT_GRAPH_NAME = "";
    String NODECOUNT_KEY = "nodeCount";
    String RELCOUNT_KEY = "relationshipCount";

    @Configuration.Parameter
    String graphName();

    @Value.Default
    @Value.Parameter(false)
    default int readConcurrency() {
        return AlgoBaseConfig.DEFAULT_CONCURRENCY;
    }

    @Value.Default
    @Value.Parameter(false)
    @Configuration.Key(NODECOUNT_KEY)
    default long nodeCount() {
        return -1;
    }

    @Value.Default
    @Value.Parameter(false)
    @Configuration.Key(RELCOUNT_KEY)
    default long relationshipCount() {
        return -1;
    }

    @Value.Derived
    @Value.Auxiliary
    default ZonedDateTime creationTime() {
        return TimeUtil.now();
    }

    @Value.Default
    @Value.Parameter(false)
    default boolean validateRelationships() {
        return false;
    }

    @Value.Default
    @Value.Parameter(false)
    default boolean isCypher() {
        return false;
    }

    @Configuration.Ignore
    GraphStoreFactory.Supplier graphStoreFactory();

    @Configuration.Ignore
    default Class<? extends GraphStoreFactory> getGraphImpl() {
        return isCypher()
            ? CypherFactory.class
            : NativeFactory.class;
    };

    static GraphCreateConfig createImplicit(String username, CypherMapWrapper config) {
        CypherMapWrapper.PairResult result = config.verifyMutuallyExclusivePairs(
            NODE_PROJECTION_KEY,
            RELATIONSHIP_PROJECTION_KEY,
            NODE_QUERY_KEY,
            RELATIONSHIP_QUERY_KEY,
            "Missing information for implicit graph creation."
        );
        if (result == CypherMapWrapper.PairResult.FIRST_PAIR) {
            return GraphCreateFromStoreConfig.fromProcedureConfig(username, config);
        } else {
            return GraphCreateFromCypherConfig.fromProcedureConfig(username, config);
        }
    }
}
