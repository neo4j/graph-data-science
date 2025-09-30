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
package org.neo4j.gds.community;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.ProgressTrackerFactory;
import org.neo4j.gds.approxmaxkcut.ApproxMaxKCutParameters;
import org.neo4j.gds.async.AsyncAlgorithmCaller;
import org.neo4j.gds.cliquecounting.CliqueCountingMode;
import org.neo4j.gds.cliquecounting.CliqueCountingParameters;
import org.neo4j.gds.conductance.ConductanceParameters;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.hdbscan.HDBScanParameters;
import org.neo4j.gds.k1coloring.K1ColoringParameters;
import org.neo4j.gds.kcore.KCoreDecompositionParameters;
import org.neo4j.gds.kmeans.KmeansParameters;
import org.neo4j.gds.kmeans.SamplerType;
import org.neo4j.gds.labelpropagation.LabelPropagationParameters;
import org.neo4j.gds.leiden.LeidenParameters;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.triangle.LocalClusteringCoefficientParameters;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@GdlExtension
class CommunityComputeFacadeTest {

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
        (a)-[:REL]->(c)
        """;

    @Inject
    private TestGraph graph;

    @Inject
    private IdFunction idFunction;
    private CommunityComputeFacade facade;

    @BeforeEach
    void setUp() {
        when(progressTrackerFactoryMock.nullTracker())
            .thenReturn(ProgressTracker.NULL_TRACKER);
        when(progressTrackerFactoryMock.create(any(), any(), any(), anyBoolean()))
            .thenReturn(progressTrackerMock);

        facade = new CommunityComputeFacade(
            new AsyncAlgorithmCaller(Executors.newSingleThreadExecutor(), logMock),
            progressTrackerFactoryMock,
            TerminationFlag.RUNNING_TRUE
        );
    }

    @Test
    void maxKCut() {
        var future = facade.approxMaxKCut(
            graph,
            new ApproxMaxKCutParameters(
                (byte) 2,
                5,
                1,
                new Concurrency(4),
                10_000,
                Optional.empty(),
                List.of(),
                false,
                false
            ),
            jobIdMock,
            false
        );

        var results = future.join();

        assertThat(results.result().candidateSolution().toArray()).containsOnly(0,1);
        assertThat(results.computeMillis()).isNotNegative();

    }

    @Test
    void cliqueCounting(){
        var future = facade.cliqueCounting(
            graph,
            new CliqueCountingParameters(
              CliqueCountingMode.GloballyOnly,
                List.of(),
                new Concurrency(4)
            ),
            jobIdMock,
            false
        );

        var results = future.join();

        assertThat(results.result().globalCount()).containsExactly(1);
        assertThat(results.computeMillis()).isNotNegative();
    }

    @Test
    void conductance(){
        var future = facade.conductance(
            graph,
            new ConductanceParameters(
               new Concurrency(4),
                10_000,
                false,
                "prop"
            ),
            jobIdMock,
            false
        );

        var results = future.join();

        assertThat(results.result().globalAverageConductance()).isGreaterThan(0d);
        assertThat(results.computeMillis()).isNotNegative();
    }

    @Test
    void hdbscan(){
        var future = facade.hdbscan(
            graph,
            new HDBScanParameters(
                new Concurrency(4),
                1,
                3,
                1,
                "prop2"
            ),
            jobIdMock,
            false
        );

        var results = future.join();

        assertThat(results.result().labels().toArray()).hasSize(3);
        assertThat(results.computeMillis()).isNotNegative();
    }

    @Test
    void k1Coloring(){
        var future = facade.k1Coloring(
            graph,
            new K1ColoringParameters(
                new Concurrency(4),
                1,
                10_000
            ),
            jobIdMock,
            false
        );

        var results = future.join();

        assertThat(results.result().ranIterations()).isGreaterThan(0);
        assertThat(results.computeMillis()).isNotNegative();
    }

    @Test
    void kCore(){
        var future = facade.kCore(
            graph,
            new KCoreDecompositionParameters(
                new Concurrency(4)
            ),
            jobIdMock,
            false
        );

        var results = future.join();

        assertThat(results.result().degeneracy()).isGreaterThan(0);
        assertThat(results.computeMillis()).isNotNegative();
    }

    @Test
    void kMeans(){
        var future = facade.kMeans(
            graph,
            new KmeansParameters(
                3,
                10,
                0.5,
                1,
                false,
                new Concurrency(4),
                "prop2",
                SamplerType.UNIFORM,
                List.of(),
                Optional.empty()
            ),
            jobIdMock,
            false
        );

        var results = future.join();

        assertThat(results.result().communities().toArray()).hasSize(3);
        assertThat(results.computeMillis()).isNotNegative();
    }

    @Test
    void labelPropagation(){
        var future = facade.labelPropagation(
            graph,
            new LabelPropagationParameters(
                new Concurrency(4),
                10,
                null,
                null
            ),
            jobIdMock,
            false
        );

        var results = future.join();

        assertThat(results.result().ranIterations()).isGreaterThan(0);
        assertThat(results.computeMillis()).isNotNegative();
    }

    @Test
    void lcc(){
        var future = facade.lcc(
            graph,
            new LocalClusteringCoefficientParameters(
                new Concurrency(4),
                100,
                null
            ),
            jobIdMock,
            false
        );

        var results = future.join();

        assertThat(results.result().averageClusteringCoefficient()).isGreaterThan(0d);
        assertThat(results.computeMillis()).isNotNegative();
    }

    @Test
    void leiden(){
        var future = facade.leiden(
            graph,
            new LeidenParameters(
               new Concurrency(4),
                0,
                null,
                10,
                1,
                1,
                false,
                Optional.empty()
            ),
            jobIdMock,
            false
        );

        var results = future.join();

        assertThat(results.result().ranLevels()).isGreaterThan(0);
        assertThat(results.computeMillis()).isNotNegative();
    }

}
