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
package org.neo4j.gds.miscellaneous;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.async.AsyncAlgorithmCaller;
import org.neo4j.gds.collapsepath.CollapsePathParameters;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.indexinverse.InverseRelationshipsParameters;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.progress.tracking.ProgressTracker;
import org.neo4j.gds.progress.tracking.ProgressTrackerFactory;
import org.neo4j.gds.scaleproperties.ScalePropertiesParameters;
import org.neo4j.gds.scaling.ScalerFactory;
import org.neo4j.gds.scaling.ScalerType;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.undirected.ToUndirectedParameters;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@GdlExtension
class MiscellaneousComputeFacadeTest {

    @Mock(strictness = Mock.Strictness.LENIENT)
    private ProgressTrackerFactory progressTrackerFactoryMock;
    @Mock
    private ProgressTracker progressTrackerMock;

    @Mock
    private JobId jobIdMock;

    @Mock
    private Log logMock;

    @GdlGraph
    private static final String GDL = "(a:Node {prop: 5})-[r:REL]->(b:Node {prop: 10})";

    @Inject
    private Graph graph;

    @Inject
    private GraphStore graphStore;

    @Inject
    private IdFunction idFunction;

    private MiscellaneousComputeFacade facade;

    @BeforeEach
    void setUp() {
        when(progressTrackerFactoryMock.create(any(), any(), any(), anyBoolean()))
            .thenReturn(progressTrackerMock);

        facade = new MiscellaneousComputeFacade(
            new AsyncAlgorithmCaller(Executors.newSingleThreadExecutor(), logMock),
            progressTrackerFactoryMock,
            TerminationFlag.RUNNING_TRUE
        );
    }

    @Test
    void collapsePath(){
        var future = facade.collapsePath(
            graphStore,
            new CollapsePathParameters(
                new Concurrency(1),
                List.of(List.of("REL")),
                Set.of(NodeLabel.of("Node")),
                false,
                "F"
            ),
            jobIdMock
        );
        var results = future.join();
        var result = results.result();
        assertThat(result).isNotNull();
        assertThat(result.count()).isEqualTo(1);

    }

    @Test
    void inverseIndex(){
        var future = facade.indexInverse(
            graphStore,
            new InverseRelationshipsParameters(
                new Concurrency(1),
                Set.of(RelationshipType.of("REL"))
            ),
            jobIdMock,
            false
        );
        var results = future.join();
        var result = results.result();
        assertThat(result).isNotNull();
        assertThat(result).containsKey(RelationshipType.of("REL"));

    }

    @Test
    void scaleProperties() {
        var future = facade.scaleProperties(
            graph,
            new ScalePropertiesParameters(
                new Concurrency(1),
                List.of("prop"),
                ScalerFactory.of(ScalerType.Mean)
            ),
            jobIdMock,
            false
        );

        var results = future.join();
        var result = results.result();

        assertThat(result.scaledProperties().size()).isEqualTo(2);
        
        var aList =  new ArrayList<Double>();
        aList.add(result.scaledProperties().get(0)[0]);
        aList.add(result.scaledProperties().get(1)[0]);
        assertThat(aList).contains(-0.5, 0.5);
    }

    @Test
    void toUndirected(){
        var future = facade.toUndirected(
            graphStore,
            new ToUndirectedParameters(
                new Concurrency(1),
                Optional.empty(),
                "FOO",
                RelationshipType.of("REL")
            ),
            jobIdMock,
            false
        );
        var results = future.join();
        var result = results.result();
        assertThat(result).isNotNull();
        assertThat(result.count()).isEqualTo(2 );

    }

}
