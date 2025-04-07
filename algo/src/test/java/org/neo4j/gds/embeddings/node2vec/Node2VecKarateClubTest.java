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
package org.neo4j.gds.embeddings.node2vec;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.Intersections;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.modularity.TestGraphs;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class Node2VecKarateClubTest {

    @GdlGraph(orientation = Orientation.UNDIRECTED)
    private static final String GRAPH = TestGraphs.KARATE_CLUB_GRAPH;

    @Inject
    private TestGraph graph;

    @Test
    void node2Vec() {
        int embeddingDimension = 128;

        var trainParameters = new TrainParameters(
            0.5,
            0.25,
            10,
            5,
            5,
            embeddingDimension,
            EmbeddingInitializer.NORMALIZED
        );

        var samplingWalkParameters = new SamplingWalkParameters(
            List.of(),
            1,
            5,
            1.0,
            1.0,
            0.001,
            0.75,
            1000
        );

        var embeddings = Node2Vec.create(
            graph,
            new Node2VecParameters(
                samplingWalkParameters,
                trainParameters,
                new Concurrency(4),
                Optional.of(42L)
            ),
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
        ).compute().embeddings();

        var nodeCount = graph.nodeCount();
        var sims = new double[(int) nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            var e1 = embeddings.get(i).data();
            for (int j = 0; j < nodeCount; j++) {
                var e2 = embeddings.get(j).data();
                var sim = Intersections.cosine(e1, e2, embeddingDimension);
                sims[i] += sim;
            }
            sims[i] = sims[i] / nodeCount;
        }

        double averageSimilarity = Arrays.stream(sims).sum() / nodeCount;
        assertThat(averageSimilarity).isGreaterThanOrEqualTo(0.81);
    }
}
