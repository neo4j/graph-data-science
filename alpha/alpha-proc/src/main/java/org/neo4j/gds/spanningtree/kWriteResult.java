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
package org.neo4j.gds.spanningtree;

import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.StandardWriteResult;

import java.util.Map;

public final class kWriteResult extends StandardWriteResult {

    public final long effectiveNodeCount;

    public kWriteResult(
        long preProcessingMillis,
        long computeMillis,
        long writeMillis,
        long effectiveNodeCount,
        Map<String, Object> configuration
    ) {
        super(preProcessingMillis, computeMillis, 0, writeMillis, configuration);
        this.effectiveNodeCount = effectiveNodeCount;
    }

    public static class Builder extends AbstractResultBuilder<kWriteResult> {

        long effectiveNodeCount;

        Builder withEffectiveNodeCount(long effectiveNodeCount) {
            this.effectiveNodeCount = effectiveNodeCount;
            return this;
        }

        @Override
        public kWriteResult build() {
            return new kWriteResult(
                preProcessingMillis,
                computeMillis,
                writeMillis,
                effectiveNodeCount,
                config.toMap()
            );
        }
    }
}
