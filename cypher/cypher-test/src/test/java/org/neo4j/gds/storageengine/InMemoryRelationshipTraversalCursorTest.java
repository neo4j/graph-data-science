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
import org.neo4j.gds.compat.AbstractInMemoryRelationshipTraversalCursor;
import org.neo4j.gds.compat.Neo4jVersion;
import org.neo4j.gds.compat.StorageEngineProxy;
import org.neo4j.gds.core.cypher.CypherGraphStore;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.junit.annotation.DisableForNeo4jVersion;

import java.util.HashSet;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryRelationshipTraversalCursorTest extends CypherTest {

    @Neo4jGraph
    static final String DB_CYPHER = "CREATE" +
                                    "  (a:A)" +
                                    ", (b:B)" +
                                    ", (c:A)" +
                                    ", (a)-[:REL]->(b)" +
                                    ", (a)-[:REL]->(c)";

    @Inject
    IdFunction idFunction;

    AbstractInMemoryRelationshipTraversalCursor relationshipCursor;

    @Override
    protected GraphStore graphStore() {
        return new StoreLoaderBuilder()
            .api(db)
            .addNodeLabel("A")
            .addNodeLabel("B")
            .addRelationshipType("REL")
            .build()
            .graphStore();
    }

    @Override
    protected void onSetup() {
        var graphStore = new CypherGraphStore(this.graphStore);
        graphStore.initialize(tokenHolders);
        this.relationshipCursor = StorageEngineProxy.inMemoryRelationshipTraversalCursor(graphStore, tokenHolders);
    }

    @Test
    @DisableForNeo4jVersion(Neo4jVersion.V_4_1)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_2)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_3_drop31)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_3_drop40)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_3_drop41)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_3_drop42)
    void shouldTraverseRelationships() {
        var relTypeToken = tokenHolders.relationshipTypeTokens().getIdByName("REL");

        StorageEngineProxy.initRelationshipTraversalCursorForRelType(
            relationshipCursor,
            idFunction.of("a"),
            relTypeToken
        );

        assertThat(relationshipCursor.next()).isTrue();
        assertThat(relationshipCursor.sourceNodeReference()).isEqualTo(idFunction.of("a"));

        var results = new HashSet<>();
        results.add(idFunction.of("b"));
        results.add(idFunction.of("c"));
        assertThat(relationshipCursor.targetNodeReference()).isIn(results);
        results.remove(relationshipCursor.targetNodeReference());

        assertThat(relationshipCursor.next()).isTrue();
        assertThat(relationshipCursor.targetNodeReference()).isIn(results);

        assertThat(relationshipCursor.next()).isFalse();
    }

    @Test
    @DisableForNeo4jVersion(Neo4jVersion.V_4_1)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_2)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_3_drop31)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_3_drop40)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_3_drop41)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_3_drop42)
    void shouldSetCorrectIds() {
        var relTypeToken = tokenHolders.relationshipTypeTokens().getIdByName("REL");

        StorageEngineProxy.initRelationshipTraversalCursorForRelType(
            relationshipCursor,
            idFunction.of("a"),
            relTypeToken
        );

        assertThat(relationshipCursor.next()).isTrue();
        assertThat(relationshipCursor.getId()).isEqualTo(0L);

        assertThat(relationshipCursor.next()).isTrue();
        assertThat(relationshipCursor.getId()).isEqualTo(1L);
    }
}
