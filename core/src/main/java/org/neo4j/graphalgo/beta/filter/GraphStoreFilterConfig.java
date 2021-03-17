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
package org.neo4j.graphalgo.beta.filter;

import org.immutables.value.Value;
import org.neo4j.graphalgo.annotation.Configuration;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.config.BaseConfig;
import org.neo4j.graphalgo.config.ConcurrencyConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;

@ValueClass
@Configuration
@SuppressWarnings("immutables:subtype")
public interface GraphStoreFilterConfig extends BaseConfig, ConcurrencyConfig {

    @Configuration.Parameter
    String graphName();

    @Configuration.Parameter
    String subgraphName();

    @Value.Default
    default String nodeFilter() {
        return "true";
    }

    @Value.Default
    default String relationshipFilter() {
        return "true";
    }

    static GraphStoreFilterConfig of(
        String username,
        String graphName,
        String subgraphName,
        CypherMapWrapper config
    ) {
        return new GraphStoreFilterConfigImpl(
            graphName,
            subgraphName,
            username,
            config
        );
    }

}
