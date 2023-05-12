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
package org.neo4j.gds.ml.pipeline.node.classification.predict;

import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;
import org.neo4j.gds.ml.nodeClassification.NodeClassificationPredict;

import java.util.Optional;

@ValueClass
public interface NodeClassificationPipelineResult {
    HugeLongArray predictedClasses();

    Optional<HugeObjectArray<double[]>> predictedProbabilities();

    static NodeClassificationPipelineResult of(
        NodeClassificationPredict.NodeClassificationResult nodeClassificationResult,
        LocalIdMap classIdMap
    ) {
        var internalPredictions = nodeClassificationResult.predictedClasses();
        var predictions = HugeLongArray.newArray(internalPredictions.size());
        for (long i = 0; i < nodeClassificationResult.predictedClasses().size(); i++) {
            predictions.set(i, classIdMap.toOriginal(internalPredictions.get(i)));
        }
        return ImmutableNodeClassificationPipelineResult.of(
            predictions,
            nodeClassificationResult.predictedProbabilities()
        );
    }
}
