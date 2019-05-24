/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.results;

/**
 * @author mknblch
 */
public class DijkstraResult {

    public final long loadMillis;
    public final long evalMillis;
    public final long writeMillis;
    public final long nodeCount;
    public final double totalCost;

    public DijkstraResult(long loadMillis, long evalMillis, long writeMillis, long nodeCount, double totalCost) {
        this.loadMillis = loadMillis;
        this.evalMillis = evalMillis;
        this.writeMillis = writeMillis;
        this.nodeCount = nodeCount;
        this.totalCost = totalCost;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends AbstractResultBuilder<DijkstraResult>{

        protected long nodeCount = 0;
        protected double totalCosts = 0.0;

        public Builder withNodeCount(long nodeCount) {
            this.nodeCount = nodeCount;
            return this;
        }

        public Builder withTotalCosts(double totalCosts) {
            this.totalCosts = totalCosts;
            return this;
        }

        public DijkstraResult build() {
            return new DijkstraResult(loadDuration, evalDuration, writeDuration, nodeCount, totalCosts);
        }
    }
}
