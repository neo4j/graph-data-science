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
package org.neo4j.gds.applications.algorithms.similarity;

import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.applications.algorithms.execution.machinery.Synchroniser;
import org.neo4j.gds.applications.algorithms.machinery.StatsResultBuilder;
import org.neo4j.gds.applications.algorithms.machinery.StatsResultRenderer;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnBaseConfig;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnResult;
import org.neo4j.gds.similarity.filterednodesim.FilteredNodeSimilarityBaseConfig;
import org.neo4j.gds.similarity.knn.KnnBaseConfig;
import org.neo4j.gds.similarity.knn.KnnResult;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityBaseConfig;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityResult;

import java.util.Optional;

public class SimilarityAlgorithmsStatsModeBusinessFacade {
    private final InstrumentedSimilarityAlgorithms instrumentedPathFindingAlgorithms;
    private final Synchroniser synchroniser;

    SimilarityAlgorithmsStatsModeBusinessFacade(
        InstrumentedSimilarityAlgorithms instrumentedPathFindingAlgorithms,
        Synchroniser synchroniser
    ) {
        this.instrumentedPathFindingAlgorithms = instrumentedPathFindingAlgorithms;
        this.synchroniser = synchroniser;
    }

    public <RESULT> RESULT filteredKnn(
        GraphName graphName,
        FilteredKnnBaseConfig configuration,
        StatsResultBuilder<FilteredKnnResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedPathFindingAlgorithms.filteredKnn(
            graphName,
            configuration,
            Optional.empty(),
            new StatsResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> RESULT filteredNodeSimilarity(
        GraphName graphName,
        FilteredNodeSimilarityBaseConfig configuration,
        StatsResultBuilder<NodeSimilarityResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedPathFindingAlgorithms.filteredNodeSimilarity(
            graphName,
            configuration,
            Optional.empty(),
            new StatsResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> RESULT knn(
        GraphName graphName,
        KnnBaseConfig configuration,
        StatsResultBuilder<KnnResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedPathFindingAlgorithms.knn(
            graphName,
            configuration,
            Optional.empty(),
            new StatsResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> RESULT nodeSimilarity(
        GraphName graphName,
        NodeSimilarityBaseConfig configuration,
        StatsResultBuilder<NodeSimilarityResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedPathFindingAlgorithms.nodeSimilarity(
            graphName,
            configuration,
            Optional.empty(),
            new StatsResultRenderer<>(resultBuilder)
        ));
    }
}
