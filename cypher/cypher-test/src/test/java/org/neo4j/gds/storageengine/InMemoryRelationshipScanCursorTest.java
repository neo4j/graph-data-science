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
package org.neo4j.gds.storageengine;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.StoreLoaderBuilder;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.compat.AbstractInMemoryRelationshipScanCursor;
import org.neo4j.gds.compat.StorageEngineProxy;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class InMemoryRelationshipScanCursorTest extends CypherTest {

    @Neo4jGraph
    public static final String DB_CYPHER = "CREATE" +
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

    AbstractInMemoryRelationshipScanCursor relationshipScanCursor;

    @Override
    protected GraphStore graphStore() {
        return new StoreLoaderBuilder()
            .api(db)
            .graphName("test")
            .addAllRelationshipTypes(List.of("REL1", "REL2", "REL3"))
            .build()
            .graphStore();
    }

    @Override
    protected void onSetup() {
        this.relationshipScanCursor = StorageEngineProxy.inMemoryRelationshipScanCursor(graphStore, tokenHolders);
    }

    @Test
    void shouldPerformScan() {
        relationshipScanCursor.scan();

        assertThat(relationshipScanCursor.next()).isTrue();
        assertThat(relationshipScanCursor.getId()).isEqualTo(0);

        assertThat(relationshipScanCursor.next()).isTrue();
        assertThat(relationshipScanCursor.getId()).isEqualTo(1);

        assertThat(relationshipScanCursor.next()).isTrue();
        assertThat(relationshipScanCursor.getId()).isEqualTo(3);

        assertThat(relationshipScanCursor.next()).isTrue();
        assertThat(relationshipScanCursor.getId()).isEqualTo(2);

        assertThat(relationshipScanCursor.next()).isTrue();
        assertThat(relationshipScanCursor.getId()).isEqualTo(4);

        assertThat(relationshipScanCursor.next()).isFalse();
    }

    @Test
    void shouldGetSingleRelationship() {
        relationshipScanCursor.single(2);

        assertThat(relationshipScanCursor.next()).isTrue();
        assertThat(relationshipScanCursor.getId()).isEqualTo(2);
        assertThat(relationshipScanCursor.sourceNodeReference()).isEqualTo(idFunction.of("b"));
        assertThat(relationshipScanCursor.targetNodeReference()).isEqualTo(idFunction.of("c"));
    }
}
