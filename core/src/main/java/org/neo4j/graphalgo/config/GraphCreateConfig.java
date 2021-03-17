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
import org.neo4j.graphalgo.annotation.Configuration;
import org.neo4j.graphalgo.api.GraphStoreFactory;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.TimeUtil;

import java.time.ZonedDateTime;

import static org.neo4j.graphalgo.config.GraphCreateFromCypherConfig.NODE_QUERY_KEY;
import static org.neo4j.graphalgo.config.GraphCreateFromCypherConfig.RELATIONSHIP_QUERY_KEY;
import static org.neo4j.graphalgo.config.GraphCreateFromStoreConfig.NODE_PROJECTION_KEY;
import static org.neo4j.graphalgo.config.GraphCreateFromStoreConfig.RELATIONSHIP_PROJECTION_KEY;

public interface GraphCreateConfig extends BaseConfig {

    String IMPLICIT_GRAPH_NAME = "";
    String NODE_COUNT_KEY = "nodeCount";
    String RELATIONSHIP_COUNT_KEY = "relationshipCount";
    String READ_CONCURRENCY_KEY = "readConcurrency";

    @Configuration.Parameter
    String graphName();

    @Value.Default
    @Value.Parameter(false)
    @Configuration.Key(READ_CONCURRENCY_KEY)
    default int readConcurrency() {
        return ConcurrencyConfig.DEFAULT_CONCURRENCY;
    }

    @Value.Default
    @Value.Parameter(false)
    @Configuration.Key(NODE_COUNT_KEY)
    default long nodeCount() {
        return -1;
    }

    @Value.Default
    @Value.Parameter(false)
    @Configuration.Key(RELATIONSHIP_COUNT_KEY)
    default long relationshipCount() {
        return -1;
    }

    @Value.Parameter(false)
    @Configuration.Ignore
    default boolean isFictitiousLoading() {
        return nodeCount() > -1 || relationshipCount() > -1;
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

    @Configuration.Ignore
    GraphStoreFactory.Supplier graphStoreFactory();

    @Value.Check
    default void validateReadConcurrency() {
        ConcurrencyConfig.validateConcurrency(readConcurrency(), READ_CONCURRENCY_KEY);
    }

    @Configuration.Ignore
    <R> R accept(Cases<R> visitor);

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

    interface Cases<R> {
        R store(GraphCreateFromStoreConfig storeConfig);

        R cypher(GraphCreateFromCypherConfig cypherConfig);

        R graph(GraphCreateFromGraphConfig graphConfig);

        R random(RandomGraphGeneratorConfig randomGraphConfig);
    }

    interface Visitor extends Cases<Void> {

        @Override
        default Void store(GraphCreateFromStoreConfig storeConfig) {
            visit(storeConfig);
            return null;
        }

        @Override
        default Void cypher(GraphCreateFromCypherConfig cypherConfig) {
            visit(cypherConfig);
            return null;
        }

        @Override
        default Void graph(GraphCreateFromGraphConfig graphConfig) {
            visit(graphConfig);
            return null;
        }

        @Override
        default Void random(RandomGraphGeneratorConfig randomGraphConfig) {
            visit(randomGraphConfig);
            return null;
        }

        default void visit(GraphCreateFromStoreConfig storeConfig) {}

        default void visit(GraphCreateFromCypherConfig cypherConfig) {}

        default void visit(GraphCreateFromGraphConfig graphConfig) {}

        default void visit(RandomGraphGeneratorConfig randomGraphConfig) {}
    }

    interface Rewriter extends Cases<GraphCreateConfig> {

        @Override
        default GraphCreateConfig store(GraphCreateFromStoreConfig storeConfig) {
            return storeConfig;
        }

        @Override
        default GraphCreateConfig cypher(GraphCreateFromCypherConfig cypherConfig) {
            return cypherConfig;
        }

        @Override
        default GraphCreateConfig graph(GraphCreateFromGraphConfig graphConfig) {
            return graphConfig;
        }

        @Override
        default GraphCreateConfig random(RandomGraphGeneratorConfig randomGraphConfig) {
            return randomGraphConfig;
        }

        default GraphCreateConfig apply(GraphCreateConfig config) {
            return config.accept(this);
        }
    }
}
