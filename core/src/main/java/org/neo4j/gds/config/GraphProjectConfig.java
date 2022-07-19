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
package org.neo4j.gds.config;

import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.api.GraphStoreFactory;
import org.neo4j.gds.concurrency.ConcurrencyValidatorService;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.StringIdentifierValidations;
import org.neo4j.gds.core.Username;
import org.neo4j.gds.core.utils.TimeUtil;

import java.time.ZonedDateTime;

import static org.neo4j.gds.config.GraphProjectFromCypherConfig.NODE_QUERY_KEY;
import static org.neo4j.gds.config.GraphProjectFromCypherConfig.RELATIONSHIP_QUERY_KEY;
import static org.neo4j.gds.config.GraphProjectFromStoreConfig.NODE_PROJECTION_KEY;
import static org.neo4j.gds.config.GraphProjectFromStoreConfig.RELATIONSHIP_PROJECTION_KEY;

public interface GraphProjectConfig extends BaseConfig, JobIdConfig {

    String IMPLICIT_GRAPH_NAME = "";
    String NODE_COUNT_KEY = "nodeCount";
    String RELATIONSHIP_COUNT_KEY = "relationshipCount";
    String READ_CONCURRENCY_KEY = "readConcurrency";
    String VALIDATE_RELATIONSHIPS_KEY = "validateRelationships";

    @Configuration.Parameter
    @Value.Default
    default String username() {
        return Username.EMPTY_USERNAME.username();
    }

    @Configuration.Parameter
    @Configuration.ConvertWith("validateName")
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
    @Configuration.Key(VALIDATE_RELATIONSHIPS_KEY)
    default boolean validateRelationships() {
        return false;
    }

    @Configuration.Ignore
    GraphStoreFactory.Supplier graphStoreFactory();

    @Value.Check
    default void validateReadConcurrency() {
        ConcurrencyValidatorService
            .validator()
            .validate(readConcurrency(), READ_CONCURRENCY_KEY, ConcurrencyConfig.CONCURRENCY_LIMITATION);
    }

    @Configuration.Ignore
    <R> R accept(Cases<R> visitor);

    static @Nullable String validateName(String input) {
        return StringIdentifierValidations.validateNoWhiteCharacter(input, "graphName");
    }

    static GraphProjectConfig createImplicit(String username, CypherMapWrapper config) {
        CypherMapWrapper.PairResult result = config.verifyMutuallyExclusivePairs(
            NODE_PROJECTION_KEY,
            RELATIONSHIP_PROJECTION_KEY,
            NODE_QUERY_KEY,
            RELATIONSHIP_QUERY_KEY,
            "Missing information for implicit graph creation."
        );
        if (result == CypherMapWrapper.PairResult.FIRST_PAIR) {
            return GraphProjectFromStoreConfig.fromProcedureConfig(username, config);
        } else {
            return GraphProjectFromCypherConfig.fromProcedureConfig(username, config);
        }
    }

    interface Cases<R> {
        R store(GraphProjectFromStoreConfig storeConfig);

        R cypher(GraphProjectFromCypherConfig cypherConfig);

        R graph(GraphProjectFromGraphConfig graphConfig);

        R random(RandomGraphGeneratorConfig randomGraphConfig);

        R sample(GraphSampleProcConfig graphSampleProcConfig);
    }

    interface Visitor extends Cases<Void> {

        @Override
        default Void store(GraphProjectFromStoreConfig storeConfig) {
            visit(storeConfig);
            return null;
        }

        @Override
        default Void cypher(GraphProjectFromCypherConfig cypherConfig) {
            visit(cypherConfig);
            return null;
        }

        @Override
        default Void graph(GraphProjectFromGraphConfig graphConfig) {
            visit(graphConfig);
            return null;
        }

        @Override
        default Void random(RandomGraphGeneratorConfig randomGraphConfig) {
            visit(randomGraphConfig);
            return null;
        }

        @Override
        default Void sample(GraphSampleProcConfig sampleProcConfig) {
            visit(sampleProcConfig);
            return null;
        }

        default void visit(GraphProjectFromStoreConfig storeConfig) {}

        default void visit(GraphProjectFromCypherConfig cypherConfig) {}

        default void visit(GraphProjectFromGraphConfig graphConfig) {}

        default void visit(RandomGraphGeneratorConfig randomGraphConfig) {}

        default void visit(GraphSampleProcConfig sampleProcConfig) {}
    }
}
