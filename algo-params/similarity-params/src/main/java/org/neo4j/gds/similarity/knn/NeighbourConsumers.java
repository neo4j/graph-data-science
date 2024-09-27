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
package org.neo4j.gds.similarity.knn;

/**
 * A holder for {@link org.neo4j.gds.similarity.knn.NeighbourConsumer}s. This instrument helps us extend KNN.
 */
public interface NeighbourConsumers {
    /**
     * A holder for sending data into the void, which is the default behaviour in regular KNN
     */
    NeighbourConsumers no_op = nodeId -> (element, priority) -> { /* do nothing */ };

    NeighbourConsumer get(long nodeId);
}
