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
import org.neo4j.gds.NodeProjection;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.PropertyMappings;
import org.neo4j.gds.StoreLoaderBuilder;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.compat.AbstractInMemoryNodeCursor;
import org.neo4j.gds.compat.Neo4jVersion;
import org.neo4j.gds.compat.StorageEngineProxy;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.junit.annotation.DisableForNeo4jVersion;
import org.neo4j.gds.junit.annotation.EnableForNeo4jVersion;
import org.neo4j.token.api.TokenNotFoundException;
import org.neo4j.values.storable.LongValue;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryNodeCursorTest extends CypherTest {

    @Neo4jGraph
    static final String DB_CYPHER = "CREATE" +
                                    "  (a:A {prop1: 42})" +
                                    ", (b:B {prop2: 1337})" +
                                    ", (:A)" +
                                    ", (:A)";

    AbstractInMemoryNodeCursor nodeCursor;

    @Override
    protected void onSetup() {
        this.nodeCursor = StorageEngineProxy.inMemoryNodeCursor(graphStore, tokenHolders);
    }

    @Override
    protected GraphStore graphStore() {
        return new StoreLoaderBuilder()
            .api(db)
            .addNodeProjection(NodeProjection.of("A", PropertyMappings.of(PropertyMapping.of("prop1"))))
            .addNodeProjection(NodeProjection.of("B", PropertyMappings.of(PropertyMapping.of("prop2"))))
            .build()
            .graphStore();
    }

    @Test
    @DisableForNeo4jVersion(Neo4jVersion.V_4_4_9_drop10)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_4_8_drop10)
    @DisableForNeo4jVersion(Neo4jVersion.V_5_0_drop50)
    @DisableForNeo4jVersion(Neo4jVersion.V_5_0_drop60)
    void shouldScanSingle() {
        nodeCursor.single(0);
        assertThat(nodeCursor.next()).isTrue();
        assertThat(nodeCursor.next()).isFalse();
    }

    @Test
    @DisableForNeo4jVersion(Neo4jVersion.V_4_4_9_drop10)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_4_8_drop10)
    @DisableForNeo4jVersion(Neo4jVersion.V_5_0_drop50)
    @DisableForNeo4jVersion(Neo4jVersion.V_5_0_drop60)
    void shouldScanRange() {
        nodeCursor.scanRange(1, 2);
        nodeCursor.next();
        assertThat(nodeCursor.getId()).isEqualTo(1);
        assertThat(nodeCursor.next()).isTrue();
        assertThat(nodeCursor.getId()).isEqualTo(2);
        assertThat(nodeCursor.next()).isFalse();
    }

    @Test
    @DisableForNeo4jVersion(Neo4jVersion.V_4_4_9_drop10)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_4_8_drop10)
    @DisableForNeo4jVersion(Neo4jVersion.V_5_0_drop50)
    @DisableForNeo4jVersion(Neo4jVersion.V_5_0_drop60)
    void shouldScanAll() {
        nodeCursor.scan();
        graphStore.nodes().forEachNode(nodeId -> {
            assertThat(nodeCursor.next()).isTrue();
            assertThat(nodeCursor.getId()).isEqualTo(nodeId);
            return true;
        });
        assertThat(nodeCursor.next()).isFalse();
    }

    @Test
    @DisableForNeo4jVersion(Neo4jVersion.V_4_4_9_drop10)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_4_8_drop10)
    @DisableForNeo4jVersion(Neo4jVersion.V_5_0_drop50)
    @DisableForNeo4jVersion(Neo4jVersion.V_5_0_drop60)
    void testLabels() {
        graphStore.nodes().forEachNode(nodeId -> {
            nodeCursor.single(nodeId);
            nodeCursor.next();
            var labelTokens = graphStore.nodes().nodeLabels(nodeId).stream()
                .mapToLong(label -> tokenHolders.labelTokens().getIdByName(label.name))
                .toArray();
            assertThat(nodeCursor.labels()).containsExactlyInAnyOrder(labelTokens);
            return true;
        });
    }

    @Test
    @EnableForNeo4jVersion(Neo4jVersion.V_4_3)
    void shouldHaveProperties43() {
        nodeCursor.next();
        assertThat(nodeCursor.hasProperties()).isTrue();
        assertThat(nodeCursor.propertiesReference()).isEqualTo(0L);
    }

    @Test
    @DisableForNeo4jVersion(Neo4jVersion.V_4_3)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_4_9_drop10)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_4_8_drop10)
    @DisableForNeo4jVersion(Neo4jVersion.V_5_0_drop50)
    @DisableForNeo4jVersion(Neo4jVersion.V_5_0_drop60)
    void shouldHaveProperties() {
        nodeCursor.next();
        assertThat(nodeCursor.hasProperties()).isTrue();
        assertThat(nodeCursor.propertiesReference()).hasFieldOrPropertyWithValue("id", 0L);
    }

    @Test
    @DisableForNeo4jVersion(Neo4jVersion.V_4_4_9_drop10)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_4_8_drop10)
    @DisableForNeo4jVersion(Neo4jVersion.V_5_0_drop50)
    @DisableForNeo4jVersion(Neo4jVersion.V_5_0_drop60)
    void shouldTraverseProperties() throws TokenNotFoundException {
        nodeCursor.next();
        var propertyCursor = StorageEngineProxy.inMemoryNodePropertyCursor(graphStore, tokenHolders);
        nodeCursor.properties(propertyCursor);
        assertThat(propertyCursor.next()).isTrue();

        var propertyName = tokenHolders.propertyKeyTokens().getTokenById(propertyCursor.propertyKey()).name();
        var propertyValue = ((LongValue) propertyCursor.propertyValue()).longValue();
        assertThat(propertyValue).isEqualTo(graphStore.nodeProperty(propertyName).values().longValue(nodeCursor.getId()));

        assertThat(propertyCursor.next()).isFalse();
    }
}
