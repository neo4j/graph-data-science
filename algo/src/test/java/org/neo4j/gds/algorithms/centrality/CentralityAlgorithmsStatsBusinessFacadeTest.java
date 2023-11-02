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
package org.neo4j.gds.algorithms.centrality;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.algorithms.AlgorithmComputationResult;
import org.neo4j.gds.algorithms.centrality.specificfields.StandardCentralityStatisticsSpecificFields;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.MutateNodePropertyConfig;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@GdlExtension
class CentralityAlgorithmsStatsBusinessFacadeTest {

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

        var configurationMock = mock(AlgoBaseConfig.class);
        var algorithmResult = AlgorithmComputationResult.<Long>withoutAlgorithmResult(graph, graphStore);


        var businessFacade = new CentralityAlgorithmsStatsBusinessFacade(null);

        var statsResult = businessFacade.statsResult(
            algorithmResult,
            configurationMock,
            null,
            null,
            false,
            0L,
            () -> StandardCentralityStatisticsSpecificFields.EMPTY
        );

        assertThat(statsResult.algorithmSpecificFields().centralityDistribution())
            .as("Incorrect additional algorithm field value")
            .isEmpty();
    }

    @Test
    void statsWithCommunityStatistics() {


        var businessFacade = new CentralityAlgorithmsStatsBusinessFacade(null);

        var configMock = mock(MutateNodePropertyConfig.class);


        var result = HugeDoubleArray.of(0.1, 0.2, 0.3, 0.4);

        var algorithmResultMock = AlgorithmComputationResult.of(
            result,
            graph,
            graphStore,
            TerminationFlag.RUNNING_TRUE
        );

        var statsResult = businessFacade.statsResult(
            algorithmResultMock,
            configMock,
            ((r) -> r::get),
            (r,  cs) -> new StandardCentralityStatisticsSpecificFields(cs),
            false,
            50L,
            () -> StandardCentralityStatisticsSpecificFields.EMPTY
        );


        assertThat(statsResult.algorithmSpecificFields().centralityDistribution()).isEmpty();
        assertThat(statsResult.computeMillis()).isEqualTo(50);
        assertThat(statsResult.postProcessingMillis()).isGreaterThanOrEqualTo(0L);

    }

    @Test
    void statsWithoutCommunityStatistics() {

        var businessFacade = new CentralityAlgorithmsStatsBusinessFacade(null);

        var configMock = mock(MutateNodePropertyConfig.class);


        var result = HugeDoubleArray.of(0.1, 0.2, 0.3, 0.4);
        var algorithmResultMock = AlgorithmComputationResult.of(
            result,
            graph,
            graphStore,
            TerminationFlag.RUNNING_TRUE
        );

        var statsResult = businessFacade.statsResult(
            algorithmResultMock,
            configMock,
            ((r) -> r::get),
            (r,  cs) -> new StandardCentralityStatisticsSpecificFields(cs),
            true,
            50L,
            () -> StandardCentralityStatisticsSpecificFields.EMPTY
        );


        assertThat((double)statsResult.algorithmSpecificFields().centralityDistribution().get("mean")).isCloseTo(0.25,
            Offset.offset(1e-4));

        assertThat(statsResult.computeMillis()).isEqualTo(50);
        assertThat(statsResult.postProcessingMillis()).isGreaterThanOrEqualTo(0L);


    }

}
