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
package org.neo4j.internal.recordstorage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseTest;
import org.neo4j.graphalgo.NodeProjection;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.compat.Neo4jVersion;
import org.neo4j.graphalgo.config.GraphCreateFromStoreConfig;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.extension.Neo4jGraph;
import org.neo4j.graphalgo.junit.annotation.DisableForNeo4jVersion;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.token.DelegatingTokenHolder;
import org.neo4j.token.ReadOnlyTokenCreator;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.NamedToken;
import org.neo4j.token.api.TokenHolder;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryStorageEngineTest extends BaseTest {

    @Neo4jGraph
    static String DB_CYPHER = "CREATE" +
                              "  (a:A {prop1: 42})" +
                              ", (b:B {prop2: 13.37})" +
                              ", (a)-[:REL]->(b)";

    private GraphStore graphStore;
    private TokenHolders tokenHolders;
    private StorageEngine storageEngine;

    @BeforeEach
    void setup() {
        this.graphStore = new StoreLoaderBuilder()
            .api(db)
            .addNodeProjection(NodeProjection.of("A", PropertyMappings.of(PropertyMapping.of("prop1"))))
            .addNodeProjection(NodeProjection.of("B", PropertyMappings.of(PropertyMapping.of("prop2"))))
            .addRelationshipProjection(RelationshipProjection.of("REL", Orientation.NATURAL))
            .build()
            .graphStore();

        GraphStoreCatalog.set(GraphCreateFromStoreConfig.emptyWithName("", db.databaseLayout().getDatabaseName()), graphStore);

        this.tokenHolders = new TokenHolders(
            new DelegatingTokenHolder(new ReadOnlyTokenCreator(), TokenHolder.TYPE_PROPERTY_KEY),
            new DelegatingTokenHolder(new ReadOnlyTokenCreator(), TokenHolder.TYPE_LABEL),
            new DelegatingTokenHolder(new ReadOnlyTokenCreator(), TokenHolder.TYPE_RELATIONSHIP_TYPE)
        );

        this.storageEngine = InMemoryStorageEngineCompanion.create(db.databaseLayout(), tokenHolders);
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    @DisableForNeo4jVersion(Neo4jVersion.V_4_0)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_1)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_2)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_3_drop31)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_3_drop40)
    void shouldPopulateTokenHolders() throws Exception {
        storageEngine.schemaAndTokensLifecycle().init();

        var labelTokens = tokenHolders.labelTokens().getAllTokens();
        assertThat(labelTokens).extracting(NamedToken::name).containsExactlyInAnyOrder("A", "B");

        var propertyTokens = tokenHolders.propertyKeyTokens().getAllTokens();
        assertThat(propertyTokens).extracting(NamedToken::name).containsExactlyInAnyOrder("prop1", "prop2");

        var relationshipTypeTokens = tokenHolders.relationshipTypeTokens().getAllTokens();
        assertThat(relationshipTypeTokens).extracting(NamedToken::name).containsExactly("REL");
    }

}
