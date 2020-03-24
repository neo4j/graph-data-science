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

package org.neo4j.graphalgo.shortestpath;

import org.neo4j.graphalgo.annotation.Configuration;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.config.BaseConfig;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.config.RelationshipWeightConfig;
import org.neo4j.graphalgo.config.WritePropertyConfig;
import org.neo4j.graphdb.Node;

import java.util.Optional;

@ValueClass
@Configuration("ShortestPathDeltaSteppingConfigImpl")
@SuppressWarnings("immutables:subtype")
public interface ShortestPathDeltaSteppingConfig extends BaseConfig, RelationshipWeightConfig, WritePropertyConfig {

    String DEFAULT_TARGET_PROPERTY = "sssp";

    @Configuration.ConvertWith("nodeId")
    long startNode();

    double delta();

    @Override
    default String writeProperty() {
        return DEFAULT_TARGET_PROPERTY;
    }

    static long nodeId(Node node) {
        return node.getId();
    }

    static ShortestPathDeltaSteppingConfig of(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> implicitCreateConfig,
        CypherMapWrapper config
    ) {
        return new ShortestPathDeltaSteppingConfigImpl(username, graphName, implicitCreateConfig, config);
    }
}
