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
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.algorithms.AlgorithmComputationResult;
import org.neo4j.gds.algorithms.similarity.specificfields.SimilaritySpecificFieldsWithDistribution;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.similarity.SimilarityResult;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityMutateConfig;

import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@GdlExtension
class SimilarityAlgorithmsMutateBusinessFacadeTest {

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

        var configurationMock = mock(NodeSimilarityMutateConfig.class);
        var algorithmResult = AlgorithmComputationResult.<Long>withoutAlgorithmResult(graph, graphStore);

        var mutateRelationshipService = mock(MutateRelationshipService.class);

        var businessFacade = new SimilarityAlgorithmsMutateBusinessFacade(null, mutateRelationshipService);

        var mutateResult = businessFacade.mutate(
            algorithmResult,
            configurationMock,
            null,
            null,
            0L,
            () -> SimilaritySpecificFieldsWithDistribution.EMPTY,
            false,
            "foo",
            "bar"
        );

        verifyNoInteractions(mutateRelationshipService);

        assertThat(mutateResult.configuration())
            .as("The configuration should be the exact same object")
            .isSameAs(configurationMock);

        assertThat(mutateResult.algorithmSpecificFields().nodesCompared())
            .as("Incorrect additional algorithm field value: nodesCompared")
            .isEqualTo(0);

        assertThat(mutateResult.algorithmSpecificFields().relationshipsWritten())
            .as("Incorrect additional algorithm field value: relationshipsWritten")
            .isEqualTo(0);

        assertThat(mutateResult.algorithmSpecificFields().similarityDistribution())
            .as("Incorrect additional algorithm field value: empty distribution")
            .isEmpty();
    }

    @Test
    void mutateWithoutStatistics() {

        var configurationMock = mock(NodeSimilarityMutateConfig.class);
        var similarityResultStream = Pair.of(1,Stream.of(new SimilarityResult(0,1,1), new SimilarityResult(0,2,0.25) ));

        var algorithmResult = AlgorithmComputationResult.of(similarityResultStream,graph,graphStore);

        var mutateRelationshipService = new MutateRelationshipService(mock(Log.class));


        when(configurationMock.mutateProperty()).thenReturn("foo");
        when(configurationMock.mutateRelationshipType()).thenReturn("bar");

        var businessFacade = new SimilarityAlgorithmsMutateBusinessFacade(null, mutateRelationshipService);


        var mutateResult = businessFacade.mutate(
            algorithmResult,
            configurationMock,
            (result) -> SimilarityResultCompanion.computeToGraph(graph,4,1,result.getRight()),
            (result,similarityDistribution) -> new SimilaritySpecificFieldsWithDistribution(4,2,similarityDistribution),
            20L,
            () -> SimilaritySpecificFieldsWithDistribution.EMPTY,
            false,
            "foo",
            "bar"
        );


        assertThat(mutateResult.algorithmSpecificFields().relationshipsWritten()).isEqualTo(2L);
        assertThat(mutateResult.algorithmSpecificFields().nodesCompared()).isEqualTo(4L);
        assertThat(mutateResult.algorithmSpecificFields().similarityDistribution()).isEmpty();

        assertThat(mutateResult.computeMillis()).isEqualTo(20);
        assertThat(mutateResult.mutateMillis()).isGreaterThanOrEqualTo(0L);
        assertThat(mutateResult.postProcessingMillis()).isGreaterThanOrEqualTo(0L);


        var mutatedGraph=graphStore.getGraph(RelationshipType.of("foo"), Optional.of("bar"));
        assertThat(mutatedGraph.relationshipCount()).isEqualTo(2L);
        double[] expected=new double[]{-100,1,0.25};
        LongAdder counter=new LongAdder();
        mutatedGraph.forEachRelationship(0,  200,(s,t,w)->{
            assertThat(w).isCloseTo(expected[(int)t],Offset.offset(1e-3));
            counter.increment();
            return true;
        });
        assertThat(counter.longValue()).isEqualTo(2l);


    }
    @Test
    void mutateWithStatistics() {

        var configurationMock = mock(NodeSimilarityMutateConfig.class);
        var similarityResultStream = Pair.of(1,Stream.of(new SimilarityResult(0,1,1), new SimilarityResult(0,2,0.25) ));

        var algorithmResult = AlgorithmComputationResult.of(similarityResultStream,graph,graphStore);

        var mutateRelationshipService = new MutateRelationshipService(mock(Log.class));


        when(configurationMock.mutateProperty()).thenReturn("bar");
        when(configurationMock.mutateRelationshipType()).thenReturn("foo");

        var businessFacade = new SimilarityAlgorithmsMutateBusinessFacade(null, mutateRelationshipService);


        var mutateResult = businessFacade.mutate(
            algorithmResult,
            configurationMock,
            (result) -> SimilarityResultCompanion.computeToGraph(graph,4,1,result.getRight()),
            (result,similarityDistribution) -> new SimilaritySpecificFieldsWithDistribution(4,2,similarityDistribution),
            20L,
            () -> SimilaritySpecificFieldsWithDistribution.EMPTY,
            true,
            "foo",
            "bar"
        );


        assertThat(mutateResult.algorithmSpecificFields().relationshipsWritten()).isEqualTo(2L);
        assertThat(mutateResult.algorithmSpecificFields().nodesCompared()).isEqualTo(4L);
        assertThat((double)mutateResult.algorithmSpecificFields().similarityDistribution().get("mean")).isCloseTo(0.625,Offset.offset(1e-3));

        assertThat(mutateResult.computeMillis()).isEqualTo(20);
        assertThat(mutateResult.mutateMillis()).isGreaterThanOrEqualTo(0L);
        assertThat(mutateResult.postProcessingMillis()).isGreaterThanOrEqualTo(0L);


        var mutatedGraph=graphStore.getGraph(RelationshipType.of("foo"), Optional.of("bar"));
        assertThat(mutatedGraph.relationshipCount()).isEqualTo(2L);
        double[] expected=new double[]{-100,1,0.25};
        LongAdder counter=new LongAdder();
        mutatedGraph.forEachRelationship(0,  200,(s,t,w)->{
            assertThat(w).isCloseTo(expected[(int)t],Offset.offset(1e-3));
            counter.increment();
            return true;
        });
        assertThat(counter.longValue()).isEqualTo(2l);

    }
}
