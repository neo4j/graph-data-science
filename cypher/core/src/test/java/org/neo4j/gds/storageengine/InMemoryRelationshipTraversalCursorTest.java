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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseTest;
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.compat.Neo4jVersion;
import org.neo4j.graphalgo.config.GraphCreateFromStoreConfig;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.extension.IdFunction;
import org.neo4j.graphalgo.extension.Inject;
import org.neo4j.graphalgo.extension.Neo4jGraph;
import org.neo4j.graphalgo.junit.annotation.DisableForNeo4jVersion;
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.recordstorage.InMemoryStorageEngineCompanion;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.token.DelegatingTokenHolder;
import org.neo4j.token.ReadOnlyTokenCreator;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenHolder;

import java.util.HashSet;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryRelationshipTraversalCursorTest extends BaseTest {

    @Neo4jGraph
    static final String DB_CYPHER = "CREATE" +
                                    "  (a:A)" +
                                    ", (b:B)" +
                                    ", (c:A)" +
                                    ", (a)-[:REL]->(b)" +
                                    ", (a)-[:REL]->(c)";

    @Inject
    IdFunction idFunction;

    GraphStore graphStore;
    TokenHolders tokenHolders;
    InMemoryRelationshipTraversalCursor relationshipCursor;

    @BeforeEach
    void setup() throws Exception {
        this.graphStore = new StoreLoaderBuilder()
            .api(db)
            .addNodeLabel("A")
            .addNodeLabel("B")
            .addRelationshipType("REL")
            .build()
            .graphStore();

        GraphStoreCatalog.set(GraphCreateFromStoreConfig.emptyWithName("", db.databaseLayout().getDatabaseName()), graphStore);

        this.tokenHolders = new TokenHolders(
            new DelegatingTokenHolder(new ReadOnlyTokenCreator(), TokenHolder.TYPE_PROPERTY_KEY),
            new DelegatingTokenHolder(new ReadOnlyTokenCreator(), TokenHolder.TYPE_LABEL),
            new DelegatingTokenHolder(new ReadOnlyTokenCreator(), TokenHolder.TYPE_RELATIONSHIP_TYPE)
        );

        InMemoryStorageEngineCompanion.create(db.databaseLayout(), tokenHolders).schemaAndTokensLifecycle().init();
        this.relationshipCursor = new InMemoryRelationshipTraversalCursor(graphStore, tokenHolders);
    }

    @Test
    @DisableForNeo4jVersion(Neo4jVersion.V_4_0)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_1)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_2)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_3_drop31)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_3_drop40)
    void shouldTraverseRelationships() {
        var relationshipSelection = RelationshipSelection.selection(
            tokenHolders.relationshipTypeTokens().getIdByName("REL"),
            Direction.OUTGOING
        );
        relationshipCursor.init(idFunction.of("a"), -1, relationshipSelection);

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
}
