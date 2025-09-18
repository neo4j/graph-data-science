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
package org.neo4j.gds.embeddings.validation;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.loading.validation.GraphValidation;
import org.neo4j.gds.utils.StringFormatting;

import static java.lang.Math.multiplyExact;

public class Node2VecGraphValidation implements GraphValidation {
    private final int walksPerNode;
    private final int walkLength;

    public Node2VecGraphValidation(int walksPerNode, int walkLength) {
        this.walksPerNode = walksPerNode;
        this.walkLength = walkLength;
    }

    @Override
    public void validate(Graph graph) {
        try {
            var ignored = multiplyExact(
                multiplyExact(graph.nodeCount(), walksPerNode),
                walkLength
            );
        } catch (ArithmeticException ex) {
            throw new IllegalArgumentException(
                StringFormatting.formatWithLocale(
                    "Aborting execution, running with the configured parameters is likely to overflow: node count: %d, walks per node: %d, walkLength: %d." +
                        " Try reducing these parameters or run on a smaller graph.",
                    graph.nodeCount(),
                    walksPerNode,
                    walkLength
                ));
        }
    }
}
