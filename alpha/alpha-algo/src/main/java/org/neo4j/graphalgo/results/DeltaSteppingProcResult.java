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
package org.neo4j.graphalgo.results;

import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.result.AbstractResultBuilder;

public class DeltaSteppingProcResult {

    public final long loadDuration;
    public final long evalDuration;
    public final long writeDuration;
    public final long nodeCount;

    public DeltaSteppingProcResult(long loadDuration, long evalDuration, long writeDuration, long nodeCount) {
        this.loadDuration = loadDuration;
        this.evalDuration = evalDuration;
        this.writeDuration = writeDuration;
        this.nodeCount = nodeCount;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends AbstractResultBuilder<DeltaSteppingProcResult> {

        protected long nodeCount = 0;

        public ProgressTimer load() {
            return ProgressTimer.start(res -> createMillis = res);
        }

        public ProgressTimer eval() {
            return ProgressTimer.start(res -> computeMillis = res);
        }

        public ProgressTimer write() {
            return ProgressTimer.start(res -> writeMillis = res);
        }

        public Builder withNodeCount(long nodeCount) {
            this.nodeCount = nodeCount;
            return this;
        }

        public DeltaSteppingProcResult build() {
            return new DeltaSteppingProcResult(createMillis, computeMillis, writeMillis, nodeCount);
        }
    }
}
