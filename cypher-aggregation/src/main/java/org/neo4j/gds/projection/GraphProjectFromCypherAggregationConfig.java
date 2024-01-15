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
package org.neo4j.gds.projection;

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.api.GraphStoreFactory;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.values.virtual.MapValue;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.neo4j.gds.Orientation.NATURAL;

@Configuration
public interface GraphProjectFromCypherAggregationConfig extends GraphProjectConfig {

    @Configuration.Ignore
    default Map<String, Object> asProcedureResultConfigurationField() {
        var result = cleansed(toMap(), outputFieldDenylist());
        result.put("query", query());
        return result;
    }

    @Configuration.Ignore
    default Orientation orientation() {
        return NATURAL;
    }

    @Configuration.Ignore
    default Aggregation aggregation() {
        return Aggregation.NONE;
    }

    default List<String> undirectedRelationshipTypes() {
        return List.of();
    }

    default List<String> inverseIndexedRelationshipTypes() {
        return List.of();
    }

    @Configuration.Parameter()
    String query();

    @Configuration.Ignore
    @Override
    default GraphStoreFactory.Supplier graphStoreFactory() {
        throw new UnsupportedOperationException(
            "Cypher aggregation does not work over the default graph store framework"
        );
    }

    @Configuration.Ignore
    default Set<String> outputFieldDenylist() {
        return Set.of(
            NODE_COUNT_KEY,
            RELATIONSHIP_COUNT_KEY,
            SUDO_KEY,
            VALIDATE_RELATIONSHIPS_KEY
        );
    }

    static GraphProjectFromCypherAggregationConfig of(
        String userName,
        String graphName,
        String query,
        MapValue config
    ) {
        return new GraphProjectFromCypherAggregationConfigImpl(
            query,
            userName,
            graphName,
            ValueMapWrapper.create(config)
        );
    }

    static GraphProjectFromCypherAggregationConfig of(
        String userName,
        String graphName,
        String query,
        @Nullable Map<String, Object> config
    ) {
        return new GraphProjectFromCypherAggregationConfigImpl(
            query,
            userName,
            graphName,
            CypherMapWrapper.create(config)
        );
    }
}
