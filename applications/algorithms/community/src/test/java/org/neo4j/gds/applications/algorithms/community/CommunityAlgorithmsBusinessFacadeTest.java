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
package org.neo4j.gds.applications.algorithms.community;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.TestProgressTracker;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.logging.LoggerForProgressTrackingAdapter;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.logging.GdsTestLog;
import org.neo4j.gds.wcc.WccStreamConfigImpl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.compat.TestLog.WARN;

class CommunityAlgorithmsBusinessFacadeTest {

    @Test
    void shouldWarnAboutThresholdOnUnweightedGraphs() {
        // I think we can move the check inside the Wcc algorithm and test it in WccTest.java as before
        // for the moment I leave as is.
        var graph = mock(Graph.class);
        var config  = WccStreamConfigImpl.builder().relationshipWeightProperty("weights").build();

        var algorithms =  mock(CommunityAlgorithms.class);

        var log = new GdsTestLog();
        var progressCreator = mock(ProgressTrackerCreator.class);

        when(progressCreator.createProgressTracker(any(),any())).thenReturn(
            TestProgressTracker.create(
                Tasks.leaf("foo",2),
                new LoggerForProgressTrackingAdapter(log),
                new Concurrency(1),
                TaskRegistryFactory.empty()
                )
        );

        var businessFacade = new CommunityAlgorithmsBusinessFacade(algorithms,progressCreator);
        businessFacade.wcc(graph,config);

        Assertions.assertThat(log.getMessages(WARN))
            .extracting(removingThreadId())
            .containsExactly("foo :: Specifying a `relationshipWeightProperty` has no effect unless `threshold` is also set.");
    }

}
