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
package org.neo4j.gds.applications.algorithms.embeddings;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.EmptyUserLogRegistryFactory;
import org.neo4j.gds.embeddings.node2vec.Node2VecStreamConfigImpl;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.logging.GdsTestLog;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;

@GdlExtension
class NodeEmbeddingAlgorithmsTest {
    @SuppressWarnings("unused")
    @GdlGraph
    private static final String DB_CYPHER =
        "CREATE" +
            "  (a:Node1)" +
            ", (b:Node1)" +
            ", (c:Node2)" +
            ", (d:Isolated)" +
            ", (e:Isolated)" +
            ", (a)-[:REL {prop: 1.0}]->(b)" +
            ", (b)-[:REL {prop: 1.0}]->(a)" +
            ", (a)-[:REL {prop: 1.0}]->(c)" +
            ", (c)-[:REL {prop: 1.0}]->(a)" +
            ", (b)-[:REL {prop: 1.0}]->(c)" +
            ", (c)-[:REL {prop: 1.0}]->(b)";

    @SuppressWarnings("unused")
    @Inject
    private Graph graph;

    @SuppressWarnings("unused")
    @Inject
    private GraphStore graphStore;

    @Test
    void shouldLogProgressForNode2Vec() {
        var log = new GdsTestLog();
        var requestScopedDependencies = RequestScopedDependencies.builder()
            .with(EmptyTaskRegistryFactory.INSTANCE)
            .with(EmptyUserLogRegistryFactory.INSTANCE)
            .build();
        var progressTrackerCreator = new ProgressTrackerCreator(log, requestScopedDependencies);
        var nodeEmbeddingAlgorithms = new NodeEmbeddingAlgorithms(null, progressTrackerCreator, null);

        var configuration = Node2VecStreamConfigImpl.builder().embeddingDimension(128).build();

        var graph = graphStore.getGraph(RelationshipType.of("REL"), Optional.empty());
        nodeEmbeddingAlgorithms.node2Vec(graph, configuration);

        assertThat(log.getMessages(TestLog.INFO))
            .extracting(removingThreadId())
            .contains(
                "Node2Vec :: Start",
                "Node2Vec :: RandomWalk :: Start",
                "Node2Vec :: RandomWalk :: create walks :: Start",
                "Node2Vec :: RandomWalk :: create walks 100%",
                "Node2Vec :: RandomWalk :: create walks :: Finished",
                "Node2Vec :: RandomWalk :: Finished",
                "Node2Vec :: train :: Start",
                "Node2Vec :: train :: iteration 1 of 1 :: Start",
                "Node2Vec :: train :: iteration 1 of 1 100%",
                "Node2Vec :: train :: iteration 1 of 1 :: Finished",
                "Node2Vec :: train :: Finished",
                "Node2Vec :: Finished"
            );
    }

    @Test
    void shouldLogProgressForNode2VecWithRelationshipWeights() {
        var log = new GdsTestLog();
        var requestScopedDependencies = RequestScopedDependencies.builder()
            .with(EmptyTaskRegistryFactory.INSTANCE)
            .with(EmptyUserLogRegistryFactory.INSTANCE)
            .build();
        var progressTrackerCreator = new ProgressTrackerCreator(log, requestScopedDependencies);
        var nodeEmbeddingAlgorithms = new NodeEmbeddingAlgorithms(null, progressTrackerCreator, null);

        var configuration = Node2VecStreamConfigImpl.builder().embeddingDimension(128).build();
        nodeEmbeddingAlgorithms.node2Vec(graph, configuration);

        assertThat(log.getMessages(TestLog.INFO))
            .extracting(removingThreadId())
            .contains(
                "Node2Vec :: Start",
                "Node2Vec :: RandomWalk :: Start",
                "Node2Vec :: RandomWalk :: DegreeCentrality :: Start",
                "Node2Vec :: RandomWalk :: DegreeCentrality :: Finished",
                "Node2Vec :: RandomWalk :: create walks :: Start",
                "Node2Vec :: RandomWalk :: create walks 100%",
                "Node2Vec :: RandomWalk :: create walks :: Finished",
                "Node2Vec :: RandomWalk :: Finished",
                "Node2Vec :: train :: Start",
                "Node2Vec :: train :: iteration 1 of 1 :: Start",
                "Node2Vec :: train :: iteration 1 of 1 100%",
                "Node2Vec :: train :: iteration 1 of 1 :: Finished",
                "Node2Vec :: train :: Finished",
                "Node2Vec :: Finished"
            );
    }
}
