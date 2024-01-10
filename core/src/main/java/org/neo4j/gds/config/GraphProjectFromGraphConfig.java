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
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.api.GraphStoreFactory;
import org.neo4j.gds.concurrency.ConcurrencyValidatorService;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.Collections;
import java.util.Map;


@Configuration
public interface GraphProjectFromGraphConfig extends GraphProjectConfig {

    @Configuration.Ignore
    default Map<String, Object> asProcedureResultConfigurationField() {
        var result = originalConfig().asProcedureResultConfigurationField();
        result.putAll(toMap());
        result.put("nodeFilter", nodeFilter());
        result.put("relationshipFilter", relationshipFilter());
        return result;
    }

    @Configuration.Parameter
    String graphName();

    @Configuration.Parameter
    String fromGraphName();

    @Configuration.Parameter
    String nodeFilter();

    @Configuration.Parameter
    String relationshipFilter();

    @Configuration.Parameter
    GraphProjectConfig originalConfig();

    default int concurrency() {
        return ConcurrencyConfig.DEFAULT_CONCURRENCY;
    }

    default Map<String, Object> parameters() {
        return Collections.emptyMap();
    }

    @Value.Check
    default void validateReadConcurrency() {
        ConcurrencyValidatorService.validator().validate(concurrency(), "concurrency", ConcurrencyConfig.CONCURRENCY_LIMITATION);
    }

    @Configuration.Ignore
    @Override
    default GraphStoreFactory.Supplier graphStoreFactory() {
        return originalConfig().graphStoreFactory();
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

    static GraphProjectFromGraphConfig of(
        String userName,
        String graphName,
        String fromGraphName,
        String nodeFilter,
        String relationshipFilter,
        GraphProjectConfig originalConfig,
        CypherMapWrapper procedureConfig
    ) {
        return new GraphProjectFromGraphConfigImpl(
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
