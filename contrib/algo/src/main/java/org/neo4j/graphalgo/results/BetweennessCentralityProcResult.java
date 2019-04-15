/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
public class BetweennessCentralityProcResult {

    public final long loadMillis;
    public final long computeMillis;
    public final long writeMillis;
    public final long nodes;
    public final double minCentrality;
    public final double maxCentrality;
    public final double sumCentrality;

    private BetweennessCentralityProcResult(Long loadMillis,
                                            Long computeMillis,
                                            Long writeMillis,
                                            Long nodes,
                                            Double centralityMin,
                                            Double centralityMax,
                                            Double centralitySum) {
        this.loadMillis = loadMillis;
        this.computeMillis = computeMillis;
        this.writeMillis = writeMillis;
        this.nodes = nodes;
        this.minCentrality = centralityMin;
        this.maxCentrality = centralityMax;
        this.sumCentrality = centralitySum;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends AbstractResultBuilder<BetweennessCentralityProcResult> {

        private long nodes = 0;
        private double centralityMin = -1;
        private double centralityMax = -1;
        private double centralitySum = -1;

        public Builder withNodeCount(long nodes) {
            this.nodes = nodes;
            return this;
        }

        public Builder withCentralityMin(double centralityMin) {
            this.centralityMin = centralityMin;
            return this;
        }

        public Builder withCentralityMax(double centralityMax) {
            this.centralityMax = centralityMax;
            return this;
        }

        public Builder withCentralitySum(double centralitySum) {
            this.centralitySum = centralitySum;
            return this;
        }

        public BetweennessCentralityProcResult build() {
            return new BetweennessCentralityProcResult(loadDuration,
                    evalDuration,
                    writeDuration,
                    nodes,
                    centralityMin,
                    centralityMax,
                    centralitySum);
        }
    }
}
