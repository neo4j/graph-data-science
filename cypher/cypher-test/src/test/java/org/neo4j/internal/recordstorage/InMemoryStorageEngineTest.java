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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.NodeProjection;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.PropertyMappings;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.StoreLoaderBuilder;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.compat.Neo4jVersion;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.junit.annotation.DisableForNeo4jVersion;
import org.neo4j.gds.storageengine.CypherTest;
import org.neo4j.token.api.NamedToken;

import static org.assertj.core.api.Assertions.assertThat;

@DisableForNeo4jVersion(Neo4jVersion.V_4_4_8_drop10)
@DisableForNeo4jVersion(Neo4jVersion.V_4_4_9_drop10)
@DisableForNeo4jVersion(Neo4jVersion.V_5_0_drop40)
class InMemoryStorageEngineTest extends CypherTest {

    @Neo4jGraph
    static String DB_CYPHER = "CREATE" +
                              "  (a:A {prop1: 42})" +
                              ", (b:B {prop2: 13.37})" +
                              ", (a)-[:REL]->(b)";

    @Override
    protected GraphStore graphStore() {
        return new StoreLoaderBuilder()
            .databaseService(db)
            .addNodeProjection(NodeProjection.of("A", PropertyMappings.of(PropertyMapping.of("prop1"))))
            .addNodeProjection(NodeProjection.of("B", PropertyMappings.of(PropertyMapping.of("prop2"))))
            .addRelationshipProjection(RelationshipProjection.of("REL", Orientation.NATURAL))
            .build()
            .graphStore();
    }

    @Override
    protected void onSetup() {}

    @Test
    void shouldPopulateTokenHolders() {
        var labelTokens = tokenHolders.labelTokens().getAllTokens();
        assertThat(labelTokens).extracting(NamedToken::name).containsExactlyInAnyOrder("A", "B");

        var propertyTokens = tokenHolders.propertyKeyTokens().getAllTokens();
        assertThat(propertyTokens).extracting(NamedToken::name).containsExactlyInAnyOrder("prop1", "prop2");

        var relationshipTypeTokens = tokenHolders.relationshipTypeTokens().getAllTokens();
        assertThat(relationshipTypeTokens).extracting(NamedToken::name).containsExactly("REL");
    }

}
