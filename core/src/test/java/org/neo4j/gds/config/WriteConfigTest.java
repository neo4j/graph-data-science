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
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.DatabaseInfo;
import org.neo4j.gds.api.DatabaseInfo.DatabaseLocation;
import org.neo4j.gds.api.properties.nodes.NodePropertyStore;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.api.schema.NodeSchema;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.huge.DirectIdMap;
import org.neo4j.gds.core.loading.Capabilities;
import org.neo4j.gds.core.loading.GraphStoreBuilder;
import org.neo4j.gds.core.loading.Nodes;
import org.neo4j.gds.core.loading.RelationshipImportResult;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThatCode;

class WriteConfigTest {

    static Stream<Arguments> baseConfigs() {
        return Stream.of(
            Arguments.of(
                TestWriteConfigImpl
                    .builder()
                    .concurrency(2)
                    .build()
            ),
            Arguments.of(
                TestWriteConfigImpl
                    .builder()
                    .concurrency(2)
                    .build()
            )
        );
    }

    @ParameterizedTest
    @EnumSource(Capabilities.WriteMode.class)
    void validateGraphStoreCapabilities(Capabilities.WriteMode writeMode) {
        var config = CypherMapWrapper.empty();
        var testConfig = new TestWriteConfigImpl(config);

        var nodes = new Nodes(NodeSchema.empty(), new DirectIdMap(0), NodePropertyStore.empty());

        var testGraphStore = new GraphStoreBuilder()
            .databaseInfo(DatabaseInfo.create(DatabaseId.of("neo4j"), DatabaseLocation.LOCAL))
            .capabilities(new Capabilities(writeMode))
            .schema(GraphSchema.mutable())
            .nodes(nodes)
            .relationshipImportResult(RelationshipImportResult.of(Map.of()))
            .concurrency(new Concurrency(1))
            .build();

        var assertion = assertThatCode(
            () -> testConfig.validateGraphIsSuitableForWrite(
                testGraphStore,
                List.of(),
                List.of()
            )
        );

        if (testGraphStore.capabilities().canWriteToLocalDatabase() || testGraphStore.capabilities()
            .canWriteToRemoteDatabase()) {
            assertion.doesNotThrowAnyException();
        } else {
            assertion
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The provided graph does not support `write` execution mode.");
        }
    }

    @ParameterizedTest
    @EnumSource(Capabilities.WriteMode.class)
    void doNotBlockWritesWhenWritingToResultStore(Capabilities.WriteMode writeMode) {
        var testConfig = new TestWriteConfigImpl
            .Builder()
            .writeToResultStore(true)
            .build();

        var nodes = new Nodes(NodeSchema.empty(), new DirectIdMap(0), NodePropertyStore.empty());

        var testGraphStore = new GraphStoreBuilder()
            .databaseInfo(DatabaseInfo.create(DatabaseId.of("neo4j"), DatabaseLocation.LOCAL))
            .capabilities(new Capabilities(writeMode))
            .schema(GraphSchema.mutable())
            .nodes(nodes)
            .relationshipImportResult(RelationshipImportResult.of(Map.of()))
            .concurrency(new Concurrency(1))
            .build();

        assertThatCode(
            () -> testConfig.validateGraphIsSuitableForWrite(
                testGraphStore,
                List.of(),
                List.of()
            )
        ).doesNotThrowAnyException();
    }

    @ParameterizedTest
    @MethodSource("baseConfigs")
    void shouldCreateFromExistingConfig(TestWriteConfig baseConfig) {
        assertThatCode(() -> TestWriteConfigImpl.Builder.from(baseConfig).build()).doesNotThrowAnyException();
    }

    @Configuration
    interface TestWriteConfig extends ConcurrencyConfig, WriteConfig {
    }
}
