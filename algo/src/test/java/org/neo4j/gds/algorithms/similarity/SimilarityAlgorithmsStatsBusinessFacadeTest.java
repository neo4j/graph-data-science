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
package org.neo4j.gds.algorithms.similarity;

import org.apache.commons.lang3.tuple.Pair;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.algorithms.AlgorithmComputationResult;
import org.neo4j.gds.algorithms.SimilaritySpecificFieldsWithDistribution;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.similarity.SimilarityResult;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityMutateConfig;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@GdlExtension
class SimilarityAlgorithmsStatsBusinessFacadeTest {

    @GdlGraph
    private static final String GRAPH =
        "CREATE " +
            " (:Node), " +
            " (:Node), " +
            " (:Node), " +
            " (:Node), ";

    @Inject
    private Graph graph;

    @Inject
    private GraphStore graphStore;


    @Test
    void statsWithoutAlgorithmResult() {

        var configurationMock = mock(NodeSimilarityMutateConfig.class);
        var algorithmResult = AlgorithmComputationResult.<Long>withoutAlgorithmResult(graph, graphStore);

        var businessFacade = new SimilarityAlgorithmsStatsBusinessFacade(null);

        var statsResult = businessFacade.statsResult(
            algorithmResult,
            null,
            null,
            0,
            () -> SimilaritySpecificFieldsWithDistribution.EMPTY,
false
        );



        assertThat(statsResult.algorithmSpecificFields().nodesCompared())
            .as("Incorrect additional algorithm field value: nodesCompared")
            .isEqualTo(0);

        assertThat(statsResult.algorithmSpecificFields().relationshipsWritten())
            .as("Incorrect additional algorithm field value: relationshipsWritten")
            .isEqualTo(0);

        assertThat(statsResult.algorithmSpecificFields().similarityDistribution())
            .as("Incorrect additional algorithm field value: empty distribution")
            .isEmpty();
    }

    @Test
    void statsWithoutStatistics() {

        var similarityResultStream = Pair.of(1,Stream.of(new SimilarityResult(0,1,1), new SimilarityResult(0,2,0.25) ));

        var algorithmResult = AlgorithmComputationResult.of(similarityResultStream,graph,graphStore, TerminationFlag.RUNNING_TRUE);

        var businessFacade = new SimilarityAlgorithmsStatsBusinessFacade(null);


        var statsResult = businessFacade.statsResult(
            algorithmResult,
            (result) -> SimilarityResultCompanion.computeToGraph(graph,4,1,result.getRight()),
            (result,similarityDistribution) -> new SimilaritySpecificFieldsWithDistribution(4,2,similarityDistribution),
            20L,
            () -> SimilaritySpecificFieldsWithDistribution.EMPTY,
            false
        );


        assertThat(statsResult.algorithmSpecificFields().relationshipsWritten()).isEqualTo(2L);
        assertThat(statsResult.algorithmSpecificFields().nodesCompared()).isEqualTo(4L);
        assertThat(statsResult.algorithmSpecificFields().similarityDistribution()).isEmpty();

        assertThat(statsResult.computeMillis()).isEqualTo(20);
        assertThat(statsResult.postProcessingMillis()).isGreaterThanOrEqualTo(0L);

    }
    @Test
    void mutateWithStatistics() {

        var similarityResultStream = Pair.of(1,Stream.of(new SimilarityResult(0,1,1), new SimilarityResult(0,2,0.25) ));

        var algorithmResult = AlgorithmComputationResult.of(similarityResultStream,graph,graphStore, TerminationFlag.RUNNING_TRUE);

        var businessFacade = new SimilarityAlgorithmsStatsBusinessFacade(null);


        var statsResult = businessFacade.statsResult(
            algorithmResult,
            (result) -> SimilarityResultCompanion.computeToGraph(graph,4,1,result.getRight()),
            (result,similarityDistribution) -> new SimilaritySpecificFieldsWithDistribution(4,2,similarityDistribution),
            20L,
            () -> SimilaritySpecificFieldsWithDistribution.EMPTY,
            true
        );


        assertThat(statsResult.algorithmSpecificFields().relationshipsWritten()).isEqualTo(2L);
        assertThat(statsResult.algorithmSpecificFields().nodesCompared()).isEqualTo(4L);
        assertThat((double)statsResult.algorithmSpecificFields().similarityDistribution().get("mean")).isCloseTo(0.625, Offset.offset(1e-3));

        assertThat(statsResult.computeMillis()).isEqualTo(20);
        assertThat(statsResult.postProcessingMillis()).isGreaterThanOrEqualTo(0L);


    }
}
