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
package org.neo4j.gds.ml.linkmodels.pipeline.predict;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNoException;

@GdlExtension
class LPGraphStoreFilterFactoryTest {

    @GdlGraph
    @GdlGraph(orientation = Orientation.UNDIRECTED, graphNamePrefix = "undirected")
    private static final String GDL =
        "CREATE" +
        "  (a:N)" +
        ", (b:N)" +
        ", (c:N)" +
        ", (a)-[:R]->(b)" +
        ", (a)-[:R1]->(b)" +
        ", (b)-[:R1]->(c)";

    @Inject
    private GraphStore graphStore;

    @Inject
    private GraphStore undirectedGraphStore;

    @Test
    void shouldFailOnDirectedRelationships() {
        var predictedRelationships = List.of(RelationshipType.of("R"), RelationshipType.of("R1"));
        assertThatIllegalArgumentException()
            .isThrownBy(
                () -> LPGraphStoreFilterFactory.validateGraphFilter(graphStore, predictedRelationships)
            )
            .withMessage(
                "Procedure requires all relationships of ['R', 'R1'] to be UNDIRECTED, but found ['R', 'R1'] to be directed.");
    }

    @Test
    void shouldNotFailOnUndirectedRelationships() {
        var predictedRelationships = List.of(RelationshipType.of("R"), RelationshipType.of("R1"));
        assertThatNoException()
            .isThrownBy(
                () -> LPGraphStoreFilterFactory.validateGraphFilter(undirectedGraphStore, predictedRelationships)
            );
    }
}
