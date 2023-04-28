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
package org.neo4j.gds.embeddings.node2vec;

import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.StandardMutateResult;

import java.util.List;
import java.util.Map;

public final class MutateResult extends StandardMutateResult {

    public final long nodeCount;
    public final long nodePropertiesWritten;
    public final List<Double> lossPerIteration;

    private MutateResult(
        long nodeCount,
        long nodePropertiesWritten,
        long preProcessingMillis,
        long computeMillis,
        long mutateMillis,
        Map<String, Object> configuration,
        List<Double> lossPerIteration
    ) {
        super(preProcessingMillis, computeMillis, 0, mutateMillis, configuration);
        this.nodeCount = nodeCount;
        this.nodePropertiesWritten = nodePropertiesWritten;
        this.lossPerIteration = lossPerIteration;
    }

    static class Builder extends AbstractResultBuilder<MutateResult> {

        private List<Double> lossPerIteration;

        @Override
        public MutateResult build() {
            return new MutateResult(
                nodeCount,
                nodePropertiesWritten,
                preProcessingMillis,
                computeMillis,
                writeMillis,
                config.toMap(),
                lossPerIteration
            );
        }

        Builder withLossPerIteration(List<Double> lossPerIteration) {
            this.lossPerIteration = lossPerIteration;
            return this;
        }
    }
}
