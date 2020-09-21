/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.impl.walking;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.api.Relationships;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.IdFunction;
import org.neo4j.graphalgo.extension.Inject;

import java.util.Optional;

import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;
import static org.neo4j.graphalgo.TestSupport.fromGdl;

@GdlExtension
class TraversalToEdgeTest {

    @GdlGraph(orientation = Orientation.UNDIRECTED)
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
    private IdFunction idFunction;


    @Test
    void foo() {
        var tookRel = graphStore.getGraph(RelationshipType.of("TOOK"));

        Relationships relationships = new TraversalToEdge(new Graph[]{tookRel, tookRel}, 2).compute();

        graphStore.addRelationshipType(RelationshipType.of("SAME_DRUG"), Optional.empty(), Optional.empty(), relationships);

        String expected =
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


        assertGraphEquals(
            fromGdl(expected),
            graphStore.getGraph(RelationshipType.of("SAME_DRUG"))
        );
    }

}
