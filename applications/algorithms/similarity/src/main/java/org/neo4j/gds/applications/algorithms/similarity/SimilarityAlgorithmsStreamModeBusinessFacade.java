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
import org.neo4j.gds.applications.algorithms.machinery.StreamResultBuilder;
import org.neo4j.gds.applications.algorithms.machinery.StreamResultRenderer;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnBaseConfig;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnResult;
import org.neo4j.gds.similarity.filterednodesim.FilteredNodeSimilarityBaseConfig;
import org.neo4j.gds.similarity.knn.KnnBaseConfig;
import org.neo4j.gds.similarity.knn.KnnResult;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityBaseConfig;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityResult;

import java.util.Optional;
import java.util.stream.Stream;

public class SimilarityAlgorithmsStreamModeBusinessFacade {
    private final InstrumentedSimilarityAlgorithms instrumentedSimilarityAlgorithms;
    private final Synchroniser synchroniser;

    SimilarityAlgorithmsStreamModeBusinessFacade(
        InstrumentedSimilarityAlgorithms instrumentedSimilarityAlgorithms,
        Synchroniser synchroniser
    ) {
        this.instrumentedSimilarityAlgorithms = instrumentedSimilarityAlgorithms;
        this.synchroniser = synchroniser;
    }

    public <RESULT> Stream<RESULT> filteredKnn(
        GraphName graphName,
        FilteredKnnBaseConfig configuration,
        StreamResultBuilder<FilteredKnnResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedSimilarityAlgorithms.filteredKnn(
            graphName,
            configuration,
            Optional.empty(),
            new StreamResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> Stream<RESULT> filteredNodeSimilarity(
        GraphName graphName,
        FilteredNodeSimilarityBaseConfig configuration,
        StreamResultBuilder<NodeSimilarityResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedSimilarityAlgorithms.filteredNodeSimilarity(
            graphName,
            configuration,
            Optional.empty(),
            new StreamResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> Stream<RESULT> knn(
        GraphName graphName,
        KnnBaseConfig configuration,
        StreamResultBuilder<KnnResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedSimilarityAlgorithms.knn(
            graphName,
            configuration,
            Optional.empty(),
            new StreamResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> Stream<RESULT> nodeSimilarity(
        GraphName graphName,
        NodeSimilarityBaseConfig configuration,
        StreamResultBuilder<NodeSimilarityResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedSimilarityAlgorithms.nodeSimilarity(
            graphName,
            configuration,
            Optional.empty(),
            new StreamResultRenderer<>(resultBuilder)
        ));
    }
}
