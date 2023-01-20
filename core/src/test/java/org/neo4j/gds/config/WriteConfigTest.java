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
package org.neo4j.gds.config;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.api.schema.NodeSchema;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.huge.DirectIdMap;
import org.neo4j.gds.core.loading.GraphStoreBuilder;
import org.neo4j.gds.core.loading.ImmutableNodes;
import org.neo4j.gds.core.loading.ImmutableStaticCapabilities;
import org.neo4j.gds.core.loading.RelationshipImportResult;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatCode;

class WriteConfigTest {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void validateGraphStoreCapabilities(boolean isBackedByDatabase) {
        var config = CypherMapWrapper.empty();
        var testConfig = new TestWriteConfigImpl(config);

        var nodes = ImmutableNodes.builder()
            .idMap(new DirectIdMap(0))
            .schema(NodeSchema.empty())
            .build();

        var testGraphStore = new GraphStoreBuilder()
            .databaseId(DatabaseId.from("neo4j"))
            .capabilities(ImmutableStaticCapabilities.of(isBackedByDatabase))
            .schema(GraphSchema.empty())
            .nodes(nodes)
            .relationshipImportResult(RelationshipImportResult.of(Map.of()))
            .concurrency(1)
            .build();

        var assertion = assertThatCode(() -> testConfig.validateGraphIsSuitableForWrite(
            testGraphStore,
            List.of(),
            List.of()
        ));

        if (isBackedByDatabase) {
            assertion.doesNotThrowAnyException();
        } else {
            assertion
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The provided graph does not support `write` execution mode.");
        }
    }

    @Configuration
    interface TestWriteConfig extends WriteConfig {
    }
}
