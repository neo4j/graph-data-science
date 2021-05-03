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
package org.neo4j.gds.paths;

import org.neo4j.graphalgo.annotation.Configuration;
import org.neo4j.graphalgo.config.AlgoBaseConfig;
import org.neo4j.graphalgo.config.RelationshipWeightConfig;
import org.neo4j.graphdb.Node;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public interface ShortestPathBaseConfig extends AlgoBaseConfig, RelationshipWeightConfig, TrackRelationshipsConfig {

    String SOURCE_NODE_KEY = "sourceNode";
    String TARGET_NODE_KEY = "targetNode";

    @Configuration.ConvertWith("org.neo4j.gds.paths.ShortestPathBaseConfig#parseSourceNodeId")
    long sourceNode();

    @Configuration.ConvertWith("org.neo4j.gds.paths.ShortestPathBaseConfig#parseTargetNodeId")
    long targetNode();

    @SuppressWarnings("unused")
    static long parseSourceNodeId(Object input) {
        return parseNodeId(input, SOURCE_NODE_KEY);
    }

    @SuppressWarnings("unused")
    static long parseTargetNodeId(Object input) {
        return parseNodeId(input, TARGET_NODE_KEY);
    }

    static long parseNodeId(Object input, String field) {
        if (input instanceof Node) {
            return ((Node) input).getId();
        } else if (input instanceof Number) {
            return ((Number) input).longValue();
        }

        throw new IllegalArgumentException(formatWithLocale(
            "Expected a node or a node id for `%s`. Got %s.",
            field,
            input.getClass().getSimpleName()
        ));
    }
}
