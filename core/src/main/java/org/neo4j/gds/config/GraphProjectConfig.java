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
import org.neo4j.gds.core.StringIdentifierValidations;
import org.neo4j.gds.core.Username;
import org.neo4j.gds.core.utils.TimeUtil;

import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public interface GraphProjectConfig extends BaseConfig, JobIdConfig {

    @Configuration.Ignore
    @Value.Parameter(false)
    Map<String, Object> asProcedureResultConfigurationField();

    @Configuration.Ignore
    @Value.Parameter(false)
    default Map<String, Object> cleansed(Map<String, Object> map, Collection<String> keysToIgnore) {
        Map<String, Object> result = new HashMap<>(map);
        map.forEach((key, value) -> {
            if (keysToIgnore.contains(key)) {
                result.remove(key);
            }
        });
        return result;
    }

    String IMPLICIT_GRAPH_NAME = "";
    String NODE_COUNT_KEY = "nodeCount";
    String RELATIONSHIP_COUNT_KEY = "relationshipCount";
    String READ_CONCURRENCY_KEY = "readConcurrency";
    String VALIDATE_RELATIONSHIPS_KEY = "validateRelationships";

    static GraphProjectConfig emptyWithName(String userName, String graphName) {
        return ImmutableGraphCatalogConfig.builder()
            .username(userName)
            .graphName(graphName)
            .graphStoreFactory(loaderContext -> {
                throw new UnsupportedOperationException("GraphStoreFactory not set");
            })
            .build();
    }

    @Configuration.Parameter
    @Value.Default
    default String username() {
        return Username.EMPTY_USERNAME.username();
    }

    @Configuration.Parameter
    @Configuration.ConvertWith(method = "validateName")
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

    @Value.Auxiliary
    @Configuration.Ignore
    <R> R accept(Cases<R> visitor);

    static @Nullable String validateName(String input) {
        return StringIdentifierValidations.validateNoWhiteCharacter(input, "graphName");
    }

    interface Cases<R> {

        R graph(GraphProjectFromGraphConfig graphConfig);

        R random(RandomGraphGeneratorConfig randomGraphConfig);

        R sample(GraphSampleProcConfig graphSampleProcConfig);

        R catalog(GraphCatalogConfig graphCatalogConfig);
    }

    interface Visitor extends Cases<Void> {

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

        @Override
        default Void catalog(GraphCatalogConfig graphCatalogConfig) {
            visit(graphCatalogConfig);
            return null;
        }

        default void visit(GraphProjectFromGraphConfig graphConfig) {}

        default void visit(RandomGraphGeneratorConfig randomGraphConfig) {}

        default void visit(GraphSampleProcConfig sampleProcConfig) {}

        default void visit(GraphCatalogConfig graphCatalogConfig) {}
    }
}
