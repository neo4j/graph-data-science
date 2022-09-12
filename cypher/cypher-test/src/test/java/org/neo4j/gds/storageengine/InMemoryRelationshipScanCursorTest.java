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
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.StoreLoaderBuilder;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.compat.InMemoryPropertySelection;
import org.neo4j.gds.compat.Neo4jVersion;
import org.neo4j.gds.compat.StorageEngineProxy;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.junit.annotation.DisableForNeo4jVersion;
import org.neo4j.internal.recordstorage.AbstractInMemoryRelationshipScanCursor;
import org.neo4j.values.storable.ValueGroup;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@DisableForNeo4jVersion(Neo4jVersion.V_4_4_9_drop10)
@DisableForNeo4jVersion(Neo4jVersion.V_5_0_drop80)
@DisableForNeo4jVersion(Neo4jVersion.V_5_0_drop90)
public class InMemoryRelationshipScanCursorTest extends CypherTest {

    @Neo4jGraph
    public static final String DB_CYPHER = "CREATE" +
                                           "  (a:Label)" +
                                           ", (b:Label)" +
                                           ", (c:Label)" +
                                           ", (a)-[:REL1 {relProp: 1.0D}]->(b)" +
                                           ", (a)-[:REL1 {relProp: 2.0D}]->(c)" +
                                           ", (b)-[:REL2 {relProp: 3.0D}]->(c)" +
                                           ", (a)-[:REL3 {relProp: 4.0D}]->(a)" +
                                           ", (c)-[:REL3 {relProp: 5.0D}]->(b)";

    @Inject
    IdFunction idFunction;

    AbstractInMemoryRelationshipScanCursor relationshipScanCursor;

    @Override
    protected GraphStore graphStore() {
        return new StoreLoaderBuilder()
            .databaseService(db)
            .graphName("test")
            .addAllRelationshipTypes(List.of("REL1", "REL2", "REL3"))
            .addRelationshipProperty(PropertyMapping.of("relProp"))
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

    @Test
    void shouldSupportPropertiesForSingleRelationship() {
        var propertyCursor = StorageEngineProxy.inMemoryRelationshipPropertyCursor(graphStore, tokenHolders);

        relationshipScanCursor.single(2);

        assertThat(relationshipScanCursor.next()).isTrue();
        relationshipScanCursor.properties(propertyCursor, InMemoryPropertySelection.SELECT_ALL);

        assertThat(propertyCursor.next()).isTrue();
        assertThat(propertyCursor.propertyValue().asObject()).isEqualTo(3.0D);
        assertThat(propertyCursor.propertyType()).isEqualTo(ValueGroup.NUMBER);
        assertThat(propertyCursor.propertyKey()).isEqualTo(relationshipScanCursor.tokenHolders.propertyKeyTokens().getIdByName("relProp"));
    }

    @Test
    void shouldPerformRangeScan() {
        relationshipScanCursor.scanRange(1, 3);

        var actualRelationships = new HashMap<Long, Relationship>();
        while(relationshipScanCursor.next()) {
            actualRelationships.put(
                relationshipScanCursor.getId(),
                ImmutableRelationship.of(
                    relationshipScanCursor.sourceNodeReference(),
                    relationshipScanCursor.targetNodeReference()
                )
            );
        }

        var expectedRelationships = Map.of(
            1L, ImmutableRelationship.of(idFunction.of("a"), idFunction.of("c")),
            2L, ImmutableRelationship.of(idFunction.of("b"), idFunction.of("c")),
            3L, ImmutableRelationship.of(idFunction.of("a"), idFunction.of("a"))
        );

        assertThat(actualRelationships).containsExactlyInAnyOrderEntriesOf(expectedRelationships);
    }

    @ValueClass
    interface Relationship {
        long source();
        long target();
    }
}
