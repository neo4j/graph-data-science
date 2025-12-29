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
import org.neo4j.gds.async.AsyncAlgorithmCaller;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.pagerank.ArticleRankStatsConfigImpl;
import org.neo4j.gds.termination.TerminationFlag;

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
        (a)-[:REL]->(c)
        """;

        @Inject
        private Graph graph;

        @Inject
        private IdFunction idFunction;
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

            var config =  ArticleRankStatsConfigImpl.builder().maxIterations(3).build();
            var future = facade.articleRank(
                graph,
                config,
                jobIdMock,
                false
            );

            var results = future.join();

            assertThat(results.result().iterations()).isBetween(1,3);
            assertThat(results.computeMillis()).isNotNegative();
        }

}