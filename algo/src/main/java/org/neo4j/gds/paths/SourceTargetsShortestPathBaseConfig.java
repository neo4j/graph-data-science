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

import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.config.OptionalTargetNodeConfig;
import org.neo4j.gds.config.TargetNodesConfig;

import java.util.List;

public interface SourceTargetsShortestPathBaseConfig extends
    OptionalTargetNodeConfig,
    TargetNodesConfig,
    ShortestPathBaseConfig {

    @Configuration.Ignore
    default List<Long> targetsList() {
        var targetNode = targetNode();
        var targetNodes = targetNodes();

        if (targetNodes.isEmpty() && targetNode.isPresent()) {
            return List.of(targetNode.get());
        }
        return targetNodes;
    }

    @Configuration.Check
    default void validate() {
        if (!targetNodes().isEmpty() && targetNode().isPresent()) {
            throw new IllegalArgumentException(
                "The `targets` and `target` parameters cannot be both specified at the same time");
        }
        if (targetsList().isEmpty() && targetNode().isEmpty()) {
            throw new IllegalArgumentException(
                "One of `targets` or `target` parameters must be specified");
        }

    }


}
