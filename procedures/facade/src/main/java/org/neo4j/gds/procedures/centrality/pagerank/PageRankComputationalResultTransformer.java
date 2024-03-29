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
package org.neo4j.gds.procedures.centrality.pagerank;

import org.neo4j.gds.algorithms.NodePropertyMutateResult;
import org.neo4j.gds.algorithms.NodePropertyWriteResult;
import org.neo4j.gds.algorithms.StatsResult;
import org.neo4j.gds.algorithms.centrality.specificfields.PageRankSpecificFields;
import org.neo4j.gds.pagerank.PageRankMutateConfig;
import org.neo4j.gds.pagerank.PageRankStatsConfig;
import org.neo4j.gds.pagerank.PageRankWriteConfig;

public final class PageRankComputationalResultTransformer {

    private PageRankComputationalResultTransformer() {}

    public static PageRankStatsResult toStatsResult(
        StatsResult<PageRankSpecificFields> computationResult,
        @SuppressWarnings("TypeMayBeWeakened")
        PageRankStatsConfig configuration
    ) {
        return new PageRankStatsResult(
            computationResult.algorithmSpecificFields().ranIterations(),
            computationResult.algorithmSpecificFields().didConverge(),
            computationResult.algorithmSpecificFields().centralityDistribution(),
            computationResult.preProcessingMillis(),
            computationResult.computeMillis(),
            computationResult.postProcessingMillis(),
            configuration.toMap()
        );
    }

    public static PageRankMutateResult toMutateResult(
        NodePropertyMutateResult<PageRankSpecificFields> computationResult,
        @SuppressWarnings("TypeMayBeWeakened")
        PageRankMutateConfig config
    ) {

        return new PageRankMutateResult(
            computationResult.algorithmSpecificFields().ranIterations(),
            computationResult.algorithmSpecificFields().didConverge(),
            computationResult.algorithmSpecificFields().centralityDistribution(),
            computationResult.preProcessingMillis(),
            computationResult.computeMillis(),
            computationResult.postProcessingMillis(),
            computationResult.mutateMillis(),
            computationResult.nodePropertiesWritten(),
            config.toMap()
        );
    }


    public static PageRankWriteResult toWriteResult(
        NodePropertyWriteResult<PageRankSpecificFields> computationResult,
        @SuppressWarnings("TypeMayBeWeakened")
        PageRankWriteConfig config
    ) {

        return new PageRankWriteResult(
            computationResult.algorithmSpecificFields().ranIterations(),
            computationResult.algorithmSpecificFields().didConverge(),
            computationResult.algorithmSpecificFields().centralityDistribution(),
            computationResult.preProcessingMillis(),
            computationResult.computeMillis(),
            computationResult.postProcessingMillis(),
            computationResult.writeMillis(),
            computationResult.nodePropertiesWritten(),
            config.toMap()
        );
    }
}
