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
    Map<String, Object> asProcedureResultConfigurationField();

    @Configuration.Ignore
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
        return GraphCatalogConfigImpl.builder()
            .username(userName)
            .graphName(graphName)
            .build();
    }

    @Configuration.Parameter
    default String username() {
        return Username.EMPTY_USERNAME.username();
    }

    @Configuration.Parameter
    @Configuration.ConvertWith(method = "validateName")
    String graphName();

    @Configuration.Key(READ_CONCURRENCY_KEY)
    default int readConcurrency() {
        return ConcurrencyConfig.DEFAULT_CONCURRENCY;
    }

    @Configuration.Key(NODE_COUNT_KEY)
    default long nodeCount() {
        return -1;
    }

    @Configuration.Key(RELATIONSHIP_COUNT_KEY)
    default long relationshipCount() {
        return -1;
    }

    @Configuration.Ignore
    default boolean isFictitiousLoading() {
        return nodeCount() > -1 || relationshipCount() > -1;
    }

    default ZonedDateTime creationTime() {
        return TimeUtil.now();
    }

    @Configuration.Key(VALIDATE_RELATIONSHIPS_KEY)
    default boolean validateRelationships() {
        return false;
    }

    @Configuration.Ignore
    default GraphStoreFactory.Supplier graphStoreFactory() {
        return loaderContext -> {
            throw new UnsupportedOperationException("GraphStoreFactory not set");
        };
    }

    @Configuration.Check
    default void validateReadConcurrency() {
        ConcurrencyValidatorService
            .validator()
            .validate(readConcurrency(), READ_CONCURRENCY_KEY, ConcurrencyConfig.CONCURRENCY_LIMITATION);
    }

    static @Nullable String validateName(String input) {
        return StringIdentifierValidations.validateNoWhiteCharacter(input, "graphName");
    }
}
