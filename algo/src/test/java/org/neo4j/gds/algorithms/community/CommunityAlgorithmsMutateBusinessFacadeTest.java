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
package org.neo4j.gds.algorithms.community;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.algorithms.AlgorithmComputationResult;
import org.neo4j.gds.algorithms.StandardCommunityStatisticsSpecificFields;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.properties.nodes.NodePropertyValuesAdapter;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.config.MutateNodePropertyConfig;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.result.StatisticsComputationInstructions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@GdlExtension
class CommunityAlgorithmsMutateBusinessFacadeTest {

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
    void mutateWithoutAlgorithmResult() {

        var configurationMock = mock(MutateNodePropertyConfig.class);
        var algorithmResult = AlgorithmComputationResult.<Long>withoutAlgorithmResult(graph, graphStore);

        var nodePropertyServiceMock = mock(NodePropertyService.class);

        var businessFacade = new CommunityAlgorithmsMutateBusinessFacade(null, nodePropertyServiceMock);

        var mutateResult = businessFacade.mutateNodeProperty(
            algorithmResult,
            configurationMock,
            null,
            null,
            0L,
            () -> 19L
        );

        verifyNoInteractions(nodePropertyServiceMock);

        assertThat(mutateResult.configuration())
            .as("The configuration should be the exact same object")
            .isSameAs(configurationMock);

        assertThat(mutateResult.algorithmSpecificFields())
            .as("Incorrect additional algorithm field value")
            .isEqualTo(19L);
    }

    @Test
    void mutateWithCommunityStatistics() {

        var nodePropertyService = new NodePropertyService(mock(Log.class));

        var businessFacade = new CommunityAlgorithmsMutateBusinessFacade(null, nodePropertyService);

        var configMock = mock(MutateNodePropertyConfig.class);
        when(configMock.mutateProperty()).thenReturn("bugger-off");
        var result = HugeLongArray.newArray(graph.nodeCount());
        result.setAll(graph::toOriginalNodeId);
        var algorithmResultMock = AlgorithmComputationResult.of(result, graph, graphStore);

        var statisticsComputationInstructionsMock = mock(StatisticsComputationInstructions.class);
        when(statisticsComputationInstructionsMock.computeCountOnly()).thenReturn(true);

        CommunityAlgorithmsMutateBusinessFacade.NodePropertyValuesMapper<HugeLongArray, MutateNodePropertyConfig> nodePropertyValuesMapper =
            (r, c) -> NodePropertyValuesAdapter.adapt(r);

        var mutateResult = businessFacade.mutateNodeProperty(
            algorithmResultMock,
            configMock,
            nodePropertyValuesMapper,
            ((r) -> r::get),
            (r, cc, cs) -> new StandardCommunityStatisticsSpecificFields(
                cc,
                cs
            ),
            statisticsComputationInstructionsMock,
            50L,
            () -> StandardCommunityStatisticsSpecificFields.EMPTY
        );


        assertThat(mutateResult.algorithmSpecificFields().communityCount()).isEqualTo(4);
        assertThat(mutateResult.computeMillis()).isEqualTo(50);
        assertThat(mutateResult.mutateMillis()).isGreaterThanOrEqualTo(0L);
        assertThat(mutateResult.postProcessingMillis()).isGreaterThanOrEqualTo(0L);
        
        var nodeProperty = graphStore.nodeProperty("bugger-off");
        assertThat(nodeProperty).isNotNull();
        var buggerOffValues = nodeProperty.values();
        assertThat(buggerOffValues.nodeCount()).isEqualTo(4);
        graph.forEachNode(nodeId -> {
            assertThat(buggerOffValues.longValue(nodeId))
                .as("Mutated node property (nodeId=%s) doesn't have the expected value...", nodeId)
                .isEqualTo(graph.toOriginalNodeId(nodeId));
            return true;
        });

    }

    @Test
    void mutateWithoutCommunityStatistics() {

        var nodePropertyService = new NodePropertyService(mock(Log.class));

        var businessFacade = new CommunityAlgorithmsMutateBusinessFacade(null, nodePropertyService);

        var configMock = mock(MutateNodePropertyConfig.class);
        when(configMock.mutateProperty()).thenReturn("bugger-off");
        var result = HugeLongArray.newArray(graph.nodeCount());
        result.setAll(graph::toOriginalNodeId);
        var algorithmResultMock = AlgorithmComputationResult.of(result, graph, graphStore);

        CommunityAlgorithmsMutateBusinessFacade.NodePropertyValuesMapper<HugeLongArray, MutateNodePropertyConfig> nodePropertyValuesMapper =
            (r, c) -> NodePropertyValuesAdapter.adapt(r);

        var mutateResult = businessFacade.mutateNodeProperty(
            algorithmResultMock,
            configMock,
            nodePropertyValuesMapper,
            (r) -> 100L,
            50L,
            () -> -1L
        );

        assertThat(mutateResult.algorithmSpecificFields()).isEqualTo(100);
        assertThat(mutateResult.computeMillis()).isEqualTo(50);
        assertThat(mutateResult.mutateMillis()).isGreaterThanOrEqualTo(0L);

        var nodeProperty = graphStore.nodeProperty("bugger-off");
        assertThat(nodeProperty).isNotNull();
        var buggerOffValues = nodeProperty.values();
        assertThat(buggerOffValues.nodeCount()).isEqualTo(4);
        graph.forEachNode(nodeId -> {
            assertThat(buggerOffValues.longValue(nodeId))
                .as("Mutated node property (nodeId=%s) doesn't have the expected value...", nodeId)
                .isEqualTo(graph.toOriginalNodeId(nodeId));
            return true;
        });
    }

}
