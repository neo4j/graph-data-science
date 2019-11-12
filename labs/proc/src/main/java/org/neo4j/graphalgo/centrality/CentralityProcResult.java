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

package org.neo4j.graphalgo.centrality;

import org.neo4j.graphalgo.results.AbstractResultBuilder;

public final class CentralityProcResult {

    public final Long loadMillis;
    public final Long computeMillis;
    public final Long writeMillis;
    public final Long nodes;

    private CentralityProcResult(Long loadMillis,
                                 Long computeMillis,
                                 Long writeMillis,
                                 Long nodes) {
        this.loadMillis = loadMillis;
        this.computeMillis = computeMillis;
        this.writeMillis = writeMillis;
        this.nodes = nodes;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends AbstractResultBuilder<CentralityProcResult> {

        private long nodes = 0;
        private double centralityMin = -1;
        private double centralityMax = -1;
        private double centralitySum = -1;

        public Builder withNodeCount(long nodes) {
            this.nodes = nodes;
            return this;
        }

        public CentralityProcResult build() {
            return new CentralityProcResult(loadDuration,
                evalDuration,
                writeDuration,
                nodes);
        }
    }
}
