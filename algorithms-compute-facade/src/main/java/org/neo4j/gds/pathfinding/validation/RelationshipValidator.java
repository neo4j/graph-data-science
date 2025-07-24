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
package org.neo4j.gds.pathfinding.validation;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.partition.Partition;

import java.util.function.DoublePredicate;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

// copied from `:ml-core` so we don't bring the whole module as a dependency
class RelationshipValidator implements Runnable {

        private final Graph graph;
        private final Partition partition;
        private final DoublePredicate validator;
        private final String errorDetails;

        RelationshipValidator(
            Graph graph,
            Partition partition,
            DoublePredicate validator,
            String errorDetails
        ) {
            this.graph = graph;
            this.partition = partition;
            this.validator = validator;
            this.errorDetails = errorDetails;
        }

        @Override
        public void run() {
            partition.consume(nodeId -> graph.forEachRelationship(
                nodeId,
                Double.NaN,
                (sourceNodeId, targetNodeId, property) -> {
                    if (!validator.test(property)) {
                        throw new IllegalStateException(
                            formatWithLocale(
                                "Found an invalid relationship weight between nodes `%d` and `%d` with the property value of `%f`. %s",
                                graph.toOriginalNodeId(sourceNodeId),
                                graph.toOriginalNodeId(targetNodeId),
                                property,
                                errorDetails
                            )
                        );
                    }
                    return true;
                }
            ));

        }
    }
