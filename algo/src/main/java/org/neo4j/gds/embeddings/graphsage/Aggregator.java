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
package org.neo4j.gds.embeddings.graphsage;

import org.neo4j.gds.ml.core.Variable;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.subgraph.SubGraph;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.core.tensor.Tensor;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;

import java.util.List;
import java.util.Locale;

import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfDoubleArray;
import static org.neo4j.graphalgo.utils.StringFormatting.toUpperCaseWithLocale;

public interface Aggregator {

    Variable<Matrix> aggregate(
        Variable<Matrix> previousLayerRepresentations,
        SubGraph subGraph
    );

    // TODO: maybe turn this generic?
    List<Weights<? extends Tensor<?>>> weights();

    AggregatorType type();

    ActivationFunction activationFunction();

    enum AggregatorType {
        MEAN {
            @Override
            public MemoryRange memoryEstimation(
                long minNodeCount,
                long maxNodeCount,
                long minPreviousNodeCount,
                long maxPreviousNodeCount,
                int inputDimension,
                int embeddingDimension
            ) {
                var minBound =
                    sizeOfDoubleArray(minNodeCount * inputDimension) +
                    2 * sizeOfDoubleArray(minNodeCount * embeddingDimension);
                var maxBound =
                    sizeOfDoubleArray(maxNodeCount * inputDimension) +
                    2 * sizeOfDoubleArray(maxNodeCount * embeddingDimension);

                return MemoryRange.of(minBound, maxBound);
            }
        },
        POOL {
            @Override
            public MemoryRange memoryEstimation(
                long minNodeCount,
                long maxNodeCount,
                long minPreviousNodeCount,
                long maxPreviousNodeCount,
                int inputDimension,
                int embeddingDimension
            ) {
                var minBound =
                    3 * sizeOfDoubleArray(minPreviousNodeCount * embeddingDimension) +
                    6 * sizeOfDoubleArray(minNodeCount * embeddingDimension);
                var maxBound =
                    3 * sizeOfDoubleArray(maxPreviousNodeCount * embeddingDimension) +
                    6 * sizeOfDoubleArray(maxNodeCount * embeddingDimension);

                return MemoryRange.of(minBound, maxBound);
            }
        };

        public static AggregatorType of(String activationFunction) {
            return valueOf(toUpperCaseWithLocale(activationFunction));
        }

        public static AggregatorType parse(Object object) {
            if (object == null) {
                return null;
            }
            if (object instanceof String) {
                return of(((String) object).toUpperCase(Locale.ENGLISH));
            }
            if (object instanceof AggregatorType) {
                return (AggregatorType) object;
            }
            return null;
        }

        public static String toString(AggregatorType af) {
            return af.toString();
        }

        public abstract MemoryRange memoryEstimation(
            long minNodeCount,
            long maxNodeCount,
            long minPreviousNodeCount,
            long maxPreviousNodeCount,
            int inputDimension,
            int embeddingDimension
        );
    }
}
