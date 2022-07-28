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
package org.neo4j.gds.beta.walking;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.Relationships;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.TestSupport.fromGdl;

@GdlExtension
class CollapsePathTest {

    private static final String EXPECTED_WITHOUT_LOOPS =
        "CREATE" +
        "  (a:Patient {id: 1})" +
        "  (b:Patient {id: 2})" +
        "  (c:Patient {id: 4})" +
        "  (d:Patient {id: 5})" +

        "  (e:Drug {id: 6})" +
        "  (f:Drug {id: 7})" +

        ", (a)-[:SAME_DRUG]->(b)" +
        ", (b)-[:SAME_DRUG]->(a)" +
        ", (c)-[:SAME_DRUG]->(d)" +
        ", (d)-[:SAME_DRUG]->(c)";

    private static final String EXPECTED_WITH_LOOPS =
        "CREATE" +
        "  (a:Patient {id: 1})" +
        "  (b:Patient {id: 2})" +
        "  (c:Patient {id: 4})" +
        "  (d:Patient {id: 5})" +

        "  (e:Drug {id: 6})" +
        "  (f:Drug {id: 7})" +

        ", (e)-[:SAME_DRUG]->(e)" +
        ", (f)-[:SAME_DRUG]->(f)" +

        ", (a)-[:SAME_DRUG]->(a)" +
        ", (a)-[:SAME_DRUG]->(b)" +
        ", (b)-[:SAME_DRUG]->(b)" +
        ", (b)-[:SAME_DRUG]->(a)" +
        ", (c)-[:SAME_DRUG]->(c)" +
        ", (c)-[:SAME_DRUG]->(d)" +
        ", (d)-[:SAME_DRUG]->(d)" +
        ", (d)-[:SAME_DRUG]->(c)";

    @GdlGraph(orientation = Orientation.UNDIRECTED)
    @GdlGraph(orientation = Orientation.NATURAL, graphNamePrefix = "took")
    @GdlGraph(orientation = Orientation.REVERSE, graphNamePrefix = "takenBy")
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Patient {id: 1})" +
        "  (b:Patient {id: 2})" +
        "  (c:Patient {id: 4})" +
        "  (d:Patient {id: 5})" +

        "  (e:Drug {id: 6})" +
        "  (f:Drug {id: 7})" +

        ", (a)-[:TOOK]->(e)" +
        ", (b)-[:TOOK]->(e)" +
        ", (c)-[:TOOK]->(f)" +
        ", (d)-[:TOOK]->(f)";

    @Inject
    private GraphStore graphStore;

    @Inject
    private Graph tookGraph;
    @Inject
    private GraphStore tookGraphStore;

    @Inject
    private Graph takenByGraph;

    @Inject
    private IdFunction idFunction;

    @Test
    void testCreatingRelationships() {
        var tookRel = graphStore.getGraph(RelationshipType.of("TOOK"));

        Relationships relationships = new CollapsePath(
            Collections.singletonList(new Graph[]{tookRel, tookRel}),
            false,
            2,
            Pools.DEFAULT

        ).compute();

        assertResultGraph(graphStore, relationships, EXPECTED_WITHOUT_LOOPS);
    }

    @Test
    void testAllowCreatingSelfLoops() {
        var tookRel = graphStore.getGraph(RelationshipType.of("TOOK"));

        Relationships relationships = new CollapsePath(
            Collections.singletonList(new Graph[]{tookRel, tookRel}),
            true,
            2,
            Pools.DEFAULT

        ).compute();

        assertResultGraph(graphStore, relationships, EXPECTED_WITH_LOOPS);
    }

    @Test
    void runWithDifferentRelationshipTypes() {
        Relationships relationships = new CollapsePath(
            Collections.singletonList(new Graph[]{tookGraph, takenByGraph}),
            false,
            2,
            Pools.DEFAULT
        ).compute();

        assertResultGraph(tookGraphStore, relationships, EXPECTED_WITHOUT_LOOPS);
    }

    private void assertResultGraph(GraphStore graphStore, Relationships relationships, String expected) {
        graphStore.addRelationshipType(
            RelationshipType.of("SAME_DRUG"),
            Optional.empty(),
            Optional.empty(),
            relationships
        );

        assertGraphEquals(
            fromGdl(expected),
            graphStore.getGraph(RelationshipType.of("SAME_DRUG"))
        );
    }

    @Nested
    class LabelFiltered {

        @GdlGraph
        private static final String DB_CYPHER = "CREATE " +
            " (p1:Person)" +
            " (p2:Person)" +
            " (p3:Person)" +
            " (dog:Dog)" +
            ",(p1)-[:HAS_FRIEND]->(p2)" +
            ",(p2)-[:HAS_FRIEND]->(p3)" +
            ",(p3)-[:HAS_FRIEND]->(dog)";

        @Inject
        private GraphStore graphStore;

        @Test
        void shouldComputeForAllNodesWithoutNodeLabelsSpecified() {
            var relType = "HAS_FRIEND";
            var mutateRelType = RelationshipType.of("HAS_FRIEND_OF_FRIEND");

            // when no `nodeLabels` specified
            var config = ImmutableCollapsePathConfig.builder()
                .mutateRelationshipType(mutateRelType.name)
                .addPathTemplate(List.of(relType, relType))
                .allowSelfLoops(false)
                .build();

            var relationships = new CollapsePathAlgorithmFactory()
                .build(graphStore, config, ProgressTracker.NULL_TRACKER)
                .compute();
            graphStore.addRelationshipType(mutateRelType, Optional.empty(), Optional.empty(), relationships);
            var resultGraph = graphStore.getGraph(mutateRelType);

            // then two relationships should be created
            assertThat(resultGraph.relationshipCount()).isEqualTo(2);
            assertThat(resultGraph.exists(idFunction.of("p1"), idFunction.of("p3"))).isTrue();
            assertThat(resultGraph.exists(idFunction.of("p2"), idFunction.of("dog"))).isTrue();
        }

        @Test
        void shouldComputeForSubsetOfNodesWithNodeLabelsSpecified() {
            var relType = "HAS_FRIEND";
            var mutateRelType = RelationshipType.of("HAS_FRIEND_OF_FRIEND");

            // when Person is specified for `nodeLabels`
            var config = ImmutableCollapsePathConfig.builder()
                .addNodeLabel("Person")
                .mutateRelationshipType(mutateRelType.name)
                .addPathTemplate(List.of(relType, relType))
                .allowSelfLoops(false)
                .build();

            var relationships = new CollapsePathAlgorithmFactory()
                .build(graphStore, config, ProgressTracker.NULL_TRACKER)
                .compute();
            graphStore.addRelationshipType(mutateRelType, Optional.empty(), Optional.empty(), relationships);
            var resultGraph = graphStore.getGraph(mutateRelType);

            // a single relationship is created (there is no Dog)
            assertThat(resultGraph.relationshipCount()).isEqualTo(1);
            assertThat(resultGraph.exists(idFunction.of("p1"), idFunction.of("p3"))).isTrue();
        }
    }

}
