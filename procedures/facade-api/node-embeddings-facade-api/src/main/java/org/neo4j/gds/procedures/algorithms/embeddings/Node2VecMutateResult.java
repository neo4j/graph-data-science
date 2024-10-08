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
package org.neo4j.gds.procedures.algorithms.embeddings;

import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.procedures.algorithms.results.StandardMutateResult;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public final class Node2VecMutateResult extends StandardMutateResult {
    public final long nodeCount;
    public final long nodePropertiesWritten;
    public final List<Double> lossPerIteration;

    public Node2VecMutateResult(
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

    public static Node2VecMutateResult emptyFrom(AlgorithmProcessingTimings timings, Map<String, Object> configurationMap) {
        return new Node2VecMutateResult(
            0,
            0,
            timings.preProcessingMillis,
            timings.computeMillis,
            timings.sideEffectMillis,
            configurationMap,
            Collections.emptyList()
        );
    }

    public static class Builder extends AbstractResultBuilder<Node2VecMutateResult> {
        private List<Double> lossPerIteration;

        @Override
        public Node2VecMutateResult build() {
            return new Node2VecMutateResult(
                nodeCount,
                nodePropertiesWritten,
                preProcessingMillis,
                computeMillis,
                writeMillis,
                config.toMap(),
                lossPerIteration
            );
        }

        public Builder withLossPerIteration(List<Double> lossPerIteration) {
            this.lossPerIteration = lossPerIteration;
            return this;
        }
    }
}
