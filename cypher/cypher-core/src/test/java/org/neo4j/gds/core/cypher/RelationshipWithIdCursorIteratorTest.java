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
package org.neo4j.gds.core.cypher;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.StoreLoaderBuilder;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.extension.Neo4jGraphExtension;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Neo4jGraphExtension
class RelationshipWithIdCursorIteratorTest extends BaseTest {

    @Neo4jGraph
    public static String DB_CYPHER = "CREATE" +
                                     "  (a:Label)" +
                                     ", (b:Label)" +
                                     ", (c:Label)" +
                                     ", (a)-[:REL1]->(b)" +
                                     ", (a)-[:REL1]->(c)" +
                                     ", (b)-[:REL2]->(c)" +
                                     ", (a)-[:REL3]->(a)" +
                                     ", (c)-[:REL3]->(b)";

    @Inject
    IdFunction idFunction;

    List<RelationshipIds.RelationshipIdContext> contexts;

    @BeforeEach
    void setup() {
        var graphStore = new StoreLoaderBuilder()
            .api(db)
            .graphName("test")
            .addAllRelationshipTypes(List.of("REL1", "REL2", "REL3"))
            .build()
            .graphStore();
        var rel1 = RelationshipType.of("REL1");
        var rel2 = RelationshipType.of("REL2");
        var rel3 = RelationshipType.of("REL3");

        this.contexts = List.of(
            ImmutableRelationshipIdContext.of(
                rel1,
                2L,
                graphStore.getGraph(rel1),
                HugeLongArray.of(0)
            ),
            ImmutableRelationshipIdContext.of(
                rel2,
                1L,
                graphStore.getGraph(rel2),
                HugeLongArray.of(0)
            ),
            ImmutableRelationshipIdContext.of(
                rel3,
                2L,
                graphStore.getGraph(rel3),
                HugeLongArray.of(0, 1)
            )
        );
    }

    @Test
    void shouldProduceCorrectRelationshipIds() {
        var relationshipIterator = new RelationshipWithIdCursorIterator(contexts, idFunction.of("a"), relType -> true);

        assertThat(relationshipIterator.hasNext()).isTrue();
        assertThat(relationshipIterator.next().id()).isEqualTo(0);

        assertThat(relationshipIterator.hasNext()).isTrue();
        assertThat(relationshipIterator.next().id()).isEqualTo(1);

        assertThat(relationshipIterator.hasNext()).isTrue();
        assertThat(relationshipIterator.next().id()).isEqualTo(3);

        assertThat(relationshipIterator.hasNext()).isFalse();
    }

    @Test
    void shouldRespectRelationshipPredicate() {
        var relationshipIterator = new RelationshipWithIdCursorIterator(contexts, idFunction.of("a"), relType -> !relType.equals(RelationshipType.of("REL1")));

        assertThat(relationshipIterator.hasNext()).isTrue();
        assertThat(relationshipIterator.next().id()).isEqualTo(3);

        assertThat(relationshipIterator.hasNext()).isFalse();
    }

}
