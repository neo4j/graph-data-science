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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.StoreLoaderBuilder;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.compat.AbstractInMemoryRelationshipTraversalCursor;
import org.neo4j.gds.compat.Neo4jVersion;
import org.neo4j.gds.compat.StorageEngineProxy;
import org.neo4j.gds.core.cypher.CypherGraphStore;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.junit.annotation.DisableForNeo4jVersion;
import org.neo4j.values.storable.Values;

import java.util.HashSet;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@DisableForNeo4jVersion(Neo4jVersion.V_4_4_9_drop10)
@DisableForNeo4jVersion(Neo4jVersion.V_4_4_8_drop10)
@DisableForNeo4jVersion(Neo4jVersion.V_5_0_drop50)
@DisableForNeo4jVersion(Neo4jVersion.V_5_0_drop60)
class InMemoryRelationshipTraversalCursorTest extends CypherTest {

    @Neo4jGraph
    static final String DB_CYPHER = "CREATE" +
                                    // This node property is there on purpose to add an entry
                                    // to the token holders that is not a relationship property
                                    // in order to check that the property cursor filters these out.
                                    "  (a:A { nodeProp: 42 })" +
                                    ", (b:B)" +
                                    ", (c:A)" +
                                    ", (a)-[:REL { prop1: 42.0, prop2: 12.0 }]->(b)" +
                                    ", (a)-[:REL { prop1: 13.37, prop2: 4.2 }]->(c)";

    @Inject
    IdFunction idFunction;

    AbstractInMemoryRelationshipTraversalCursor relationshipCursor;

    @Override
    protected GraphStore graphStore() {
        return new StoreLoaderBuilder()
            .api(db)
            .addNodeLabel("A")
            .addNodeLabel("B")
            .addNodeProperty(PropertyMapping.of("nodeProp", ValueType.LONG.fallbackValue()))
            .addRelationshipType("REL")
            .addRelationshipProperty(PropertyMapping.of("prop1"))
            .addRelationshipProperty(PropertyMapping.of("prop2"))
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
    void shouldSetCorrectIdsAndTypes() {
        var relTypeToken = tokenHolders.relationshipTypeTokens().getIdByName("REL");

        StorageEngineProxy.initRelationshipTraversalCursorForRelType(
            relationshipCursor,
            idFunction.of("a"),
            relTypeToken
        );

        assertThat(relationshipCursor.next()).isTrue();
        assertThat(relationshipCursor.getId()).isEqualTo(0L);
        assertThat(relationshipCursor.getType()).isEqualTo(tokenHolders.relationshipTypeTokens().getIdByName("REL"));

        assertThat(relationshipCursor.next()).isTrue();
        assertThat(relationshipCursor.getId()).isEqualTo(1L);
        assertThat(relationshipCursor.getType()).isEqualTo(tokenHolders.relationshipTypeTokens().getIdByName("REL"));
    }

    @ParameterizedTest
    @MethodSource("propertyFilterAndExpectedValues")
    @DisableForNeo4jVersion(Neo4jVersion.V_4_3)
    void shouldGetPropertyValues(Map<String, Double> expectedValues) {
        var relTypeToken = tokenHolders.relationshipTypeTokens().getIdByName("REL");

        StorageEngineProxy.initRelationshipTraversalCursorForRelType(
            relationshipCursor,
            idFunction.of("a"),
            relTypeToken
        );

        var propertyCursor = StorageEngineProxy.inMemoryRelationshipPropertyCursor(graphStore, tokenHolders);

        assertThat(relationshipCursor.next()).isTrue();
        var propertyTokens = expectedValues
            .keySet()
            .stream()
            .mapToInt(propertyKey -> tokenHolders.propertyKeyTokens().getIdByName(propertyKey))
            .toArray();
        StorageEngineProxy.properties(relationshipCursor, propertyCursor, propertyTokens);

        expectedValues.forEach((ignore1, ignore2) -> {
            assertThat(propertyCursor.next()).isTrue();
            var actualKey = tokenHolders.propertyKeyGetName(propertyCursor.propertyKey());
            assertThat(expectedValues.keySet()).contains(actualKey);
            var actualValue = propertyCursor.propertyValue();
            var expectedValue = Values.doubleValue(expectedValues.get(actualKey));
            assertThat(actualValue).isEqualTo(expectedValue);
        });
    }

    @Test
    void shouldGetPropertyValues() {
        var relTypeToken = tokenHolders.relationshipTypeTokens().getIdByName("REL");

        StorageEngineProxy.initRelationshipTraversalCursorForRelType(
            relationshipCursor,
            idFunction.of("a"),
            relTypeToken
        );

        var propertyCursor = StorageEngineProxy.inMemoryRelationshipPropertyCursor(graphStore, tokenHolders);

        var expectedValues = Map.of("prop1", 42.0D, "prop2", 12.0D);

        assertThat(relationshipCursor.next()).isTrue();
        StorageEngineProxy.properties(relationshipCursor, propertyCursor, new int[0]);

        expectedValues.forEach((propertyKey, expectedValue) -> {
            assertThat(propertyCursor.next()).isTrue();
            assertThat(propertyCursor.propertyValue()).isEqualTo(Values.doubleValue(expectedValues.get(tokenHolders.propertyKeyGetName(propertyCursor.propertyKey()))));
        });
    }

    static Stream<Arguments> propertyFilterAndExpectedValues() {
        return Stream.of(
            Arguments.of(Map.of("prop1", 42.0D)),
            Arguments.of(Map.of("prop2", 12.0D)),
            Arguments.of(Map.of("prop1", 42.0D, "prop2", 12.0D))
        );
    }
}
