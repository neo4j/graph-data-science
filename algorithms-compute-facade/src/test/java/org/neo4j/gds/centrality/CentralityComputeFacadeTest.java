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
package org.neo4j.gds.centrality;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.ProgressTrackerFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.articulationPoints.ArticulationPointsParameters;
import org.neo4j.gds.async.AsyncAlgorithmCaller;
import org.neo4j.gds.betweenness.BetweennessCentralityParameters;
import org.neo4j.gds.bridges.BridgesParameters;
import org.neo4j.gds.closeness.ClosenessCentralityParameters;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.degree.DegreeCentralityParameters;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.harmonic.HarmonicCentralityParameters;
import org.neo4j.gds.hits.HitsConfigImpl;
import org.neo4j.gds.influenceMaximization.CELFParameters;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.pagerank.ArticleRankConfigImpl;
import org.neo4j.gds.pagerank.EigenvectorConfigImpl;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Optional;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@GdlExtension
class CentralityComputeFacadeTest {

    @Mock(strictness = Mock.Strictness.LENIENT)
    private ProgressTrackerFactory progressTrackerFactoryMock;
    @Mock
    private ProgressTracker progressTrackerMock;

    @Mock
    private JobId jobIdMock;

    @Mock
    private Log logMock;

    @GdlGraph(orientation = Orientation.UNDIRECTED)
    private static final String GDL = """
        (a:Node { prop: 1, prop2: [1.0] })-[:REL]->(b:Node { prop: 1,prop2: [2.0] }),
        (b)-[:REL]->(c:Node { prop: 3 ,prop2: [3.0]}),
        (a)-[:REL]->(c), (d:Node)-[:REL]->(e:Node), (e)-[:REL]->(f:Node)
        """;

    @Inject
    private Graph graph;

    @Inject
    private IdFunction idFunction;

    @GdlGraph(indexInverse = true, graphNamePrefix = "inverse")
    private static final String inverseGDL =GDL;

    @Inject
    private Graph inverseGraph;

    private CentralityComputeFacade facade;

    @BeforeEach
    void setUp() {
        when(progressTrackerFactoryMock.nullTracker())
            .thenReturn(ProgressTracker.NULL_TRACKER);
        when(progressTrackerFactoryMock.create(any(), any(), any(), anyBoolean()))
            .thenReturn(progressTrackerMock);

        facade = new CentralityComputeFacade(
            new AsyncAlgorithmCaller(Executors.newSingleThreadExecutor(), logMock),
            progressTrackerFactoryMock,
            TerminationFlag.RUNNING_TRUE
        );
    }

    @Test
    void articleRank() {

        var config = ArticleRankConfigImpl.builder().maxIterations(3).build();
        var future = facade.articleRank(
            graph,
            config,
            jobIdMock,
            true
        );

        var results = future.join();

        assertThat(results.result().iterations()).isBetween(1, 3);
        assertThat(results.computeMillis()).isNotNegative();
    }

    @Test
    void articulationPoints() {

        var params = new ArticulationPointsParameters(new Concurrency(1), false);
        var future = facade.articulationPoints(
            graph,
            params,
            jobIdMock,
            true
        );

        var results = future.join();

        assertThat(results.result().articulationPoints().cardinality()).isEqualTo(1L); //e is the art. point
        assertThat(results.computeMillis()).isNotNegative();
    }

    @Test
    void betweennessCentrality() {

        var params = new BetweennessCentralityParameters(new Concurrency(1), Optional.empty(), false);
        var future = facade.betweennessCentrality(
            graph,
            params,
            jobIdMock,
            true
        );

        var results = future.join();

        assertThat(results.result().centralities().size()).isEqualTo(6L);
        assertThat(results.computeMillis()).isNotNegative();
    }

    @Test
    void bridges() {

        var params = new BridgesParameters(new Concurrency(1), false);
        var future = facade.bridges(
            graph,
            params,
            jobIdMock,
            true
        );

        var results = future.join();

        assertThat(results.result().bridges()).hasSize(2); //d-e, e-f are bridges
        assertThat(results.computeMillis()).isNotNegative();
    }

    @Test
    void celf() {

        var params = new CELFParameters(2, 0.1, 10, new Concurrency(1), 10, 10);
        var future = facade.celf(
            graph,
            params,
            jobIdMock,
            true
        );

        var results = future.join();

        assertThat(results.result().totalSpread()).isGreaterThan(0);
        assertThat(results.computeMillis()).isNotNegative();
    }

    @Test
    void closenessCentrality() {

        var params = new ClosenessCentralityParameters(new Concurrency(1), false);
        var future = facade.closeness(
            graph,
            params,
            jobIdMock,
            true
        );

        var results = future.join();

        assertThat(results.result().centralities().size()).isEqualTo(6L);
        assertThat(results.computeMillis()).isNotNegative();
    }

    @Test
    void degree() {

        var params = new DegreeCentralityParameters(new Concurrency(1), Orientation.NATURAL, false, 10);
        var future = facade.degree(
            graph,
            params,
            jobIdMock,
            true
        );

        var results = future.join();

        assertThat(results.result().nodeCount()).isEqualTo(6L);
        assertThat(results.computeMillis()).isNotNegative();
    }

    @Test
    void eigenVector() {

        var config = EigenvectorConfigImpl.builder().maxIterations(3).build();
        var future = facade.eigenVector(
            graph,
            config,
            jobIdMock,
            true
        );

        var results = future.join();

        assertThat(results.result().iterations()).isBetween(1, 3);
        assertThat(results.computeMillis()).isNotNegative();
    }

    @Test
    void harmonic() {

        var params = new HarmonicCentralityParameters(new Concurrency(1));
        var future = facade.harmonic(
            graph,
            params,
            jobIdMock,
            true
        );

        var results = future.join();

        assertThat(results.result().centralities().size()).isEqualTo(6L);
        assertThat(results.computeMillis()).isNotNegative();
    }

    @Test
    void hits() {

        var config = HitsConfigImpl.builder().concurrency(4).build();

        var future = facade.hits(
            inverseGraph,
            config,
            jobIdMock,
            false
        );

        var results = future.join();

        assertThat(results.result().ranIterations()).isGreaterThan(0);
        assertThat(results.computeMillis()).isNotNegative();
    }

}