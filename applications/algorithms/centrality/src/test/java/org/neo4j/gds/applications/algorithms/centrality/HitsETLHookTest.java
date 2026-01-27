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
package org.neo4j.gds.applications.algorithms.centrality;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.core.PlainSimpleRequestCorrelationId;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.LoggerForProgressTracking;
import org.neo4j.gds.core.utils.warnings.UserLogRegistry;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.hits.HitsConfigImpl;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@GdlExtension
class HitsETLHookTest {

    @GdlGraph
    private static final String DB_CYPHER =
        "CREATE" +
            "  (a :Node)," +
            "  (b :Node)," +
            "  (a)-[:R]->(b)";

    @Inject
    GraphStore graphStore;

    @Inject
    IdFunction idFunction;

    @Test
    void shouldCreateInverseIndex(){
        var requestScopedDependencies = RequestScopedDependencies.builder()
            .correlationId(PlainSimpleRequestCorrelationId.create())
            .taskRegistryFactory(EmptyTaskRegistryFactory.INSTANCE)
            .userLogRegistry(UserLogRegistry.EMPTY)
            .build();

        var progressTrackerCreator = new ProgressTrackerCreator(LoggerForProgressTracking.noOpLog(),requestScopedDependencies);

        var hitsConfig = HitsConfigImpl
            .builder()
            .hitsIterations(10)
            .relationshipTypes(List.of("R"))
            .build();

        var hook = new HitsETLHook(hitsConfig,progressTrackerCreator,TerminationFlag.RUNNING_TRUE);

        hook.onGraphStoreLoaded(graphStore);

        var graph = graphStore.getUnion();

        assertThat(graphStore.inverseIndexedRelationshipTypes()).contains(RelationshipType.of("R"));
        assertThat(graph.degreeInverse(0L)).isEqualTo(graph.degree(1L));
        assertThat(graph.degreeInverse(1L)).isEqualTo(graph.degree(0L));
    }

    @Test
    void shouldCollectRelationshipsWithoutType(){
            var graphStore = mock(GraphStore.class);
            List<RelationshipType> relTypes  = List.of(RelationshipType.of("R1"), RelationshipType.of("R2") );
            when(graphStore.inverseIndexedRelationshipTypes()).thenReturn(Set.of( RelationshipType.of("R2"),  RelationshipType.of("R3")));

            var hook = new HitsETLHook(null,null,TerminationFlag.RUNNING_TRUE);

            assertThat(hook.relationshipsWithoutIndices(graphStore,relTypes)).containsExactly("R1");
    }

}
