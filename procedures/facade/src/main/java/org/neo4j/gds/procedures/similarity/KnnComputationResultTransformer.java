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
package org.neo4j.gds.procedures.similarity;

import org.neo4j.gds.algorithms.similarity.specificfields.KnnSpecificFields;
import org.neo4j.gds.algorithms.RelationshipWriteResult;
import org.neo4j.gds.algorithms.StatsResult;
import org.neo4j.gds.procedures.similarity.knn.KnnStatsResult;
import org.neo4j.gds.procedures.similarity.knn.KnnWriteResult;
import org.neo4j.gds.similarity.knn.KnnStatsConfig;
import org.neo4j.gds.similarity.knn.KnnWriteConfig;

final class KnnComputationResultTransformer {
    private KnnComputationResultTransformer() {}

    static KnnStatsResult toStatsResult(
        StatsResult<KnnSpecificFields> statsResult,
        KnnStatsConfig config
    ) {

        return new KnnStatsResult(
            statsResult.preProcessingMillis(),
            statsResult.computeMillis(),
            statsResult.postProcessingMillis(),
            statsResult.algorithmSpecificFields().nodesCompared(),
            statsResult.algorithmSpecificFields().relationshipsWritten(),
            statsResult.algorithmSpecificFields().similarityDistribution(),
            statsResult.algorithmSpecificFields().didConverge(),
            statsResult.algorithmSpecificFields().ranIterations(),
            statsResult.algorithmSpecificFields().nodePairsConsidered(),
            config.toMap()
        );
    }

    static KnnWriteResult toWriteResult(
        RelationshipWriteResult<KnnSpecificFields> writeResult,
        KnnWriteConfig config
    ) {

        return new KnnWriteResult(
            writeResult.preProcessingMillis(),
            writeResult.computeMillis(),
            writeResult.writeMillis(),
            writeResult.postProcessingMillis(),
            writeResult.algorithmSpecificFields().nodesCompared(),
            writeResult.algorithmSpecificFields().relationshipsWritten(),
            writeResult.algorithmSpecificFields().didConverge(),
            writeResult.algorithmSpecificFields().ranIterations(),
            writeResult.algorithmSpecificFields().nodePairsConsidered(),
            writeResult.algorithmSpecificFields().similarityDistribution(),
            config.toMap()
        );
    }
}
