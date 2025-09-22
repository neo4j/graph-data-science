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
package org.neo4j.gds.maxflow;

public interface ResidualEdgeConsumer {
    boolean accept(long s, long t, long relIdx, double residualCapacity, boolean isReverse);

    default ResidualEdgeConsumer andThen(ResidualEdgeConsumer after) {
        return (s, t, relIdx, residualCapacity, isReverse) -> {
            this.accept(s, t, relIdx, residualCapacity, isReverse);
            return after.accept(s, t, relIdx, residualCapacity, isReverse);
        };
    }
}
