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

package org.neo4j.graphalgo.impl.shortestpaths;

import org.immutables.value.Value;
import org.neo4j.graphalgo.annotation.Configuration;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.newapi.AlgoBaseConfig;
import org.neo4j.graphalgo.newapi.RelationshipWeightConfig;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;

import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.graphdb.Direction.OUTGOING;

@ValueClass
@Configuration("YensKShortestPathsConfigImpl")
public interface YensKShortestPathsConfig extends AlgoBaseConfig, RelationshipWeightConfig {

    @Configuration.ConvertWith("org.neo4j.graphalgo.Projection#parseDirection")
    @Value.Default
    default Direction direction() {
        return BOTH;
    }

    @Configuration.Ignore
    @Value.Derived
    default Direction resolvedDirection() {
        return direction() == BOTH ? OUTGOING : direction();
    }

    @Value.Default
    default boolean path() {
        return false;
    }

    @Value.Default
    default String relationshipWriteProperty() {
        return "weight";
    }

    @Value.Default
    default int maxDepth() {
        return Integer.MAX_VALUE;
    }

    int k();

    @Value.Default
    default String writePropertyPrefix() {
        return "PATH_";
    }

    @Configuration.ConvertWith("nodeId")
    long startNode();

    @Configuration.ConvertWith("nodeId")
    long endNode();

    static long nodeId(Node node) {
        return node.getId();
    }
}
