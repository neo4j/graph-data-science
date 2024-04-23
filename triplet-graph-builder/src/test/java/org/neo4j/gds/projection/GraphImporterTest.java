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
package org.neo4j.gds.projection;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.DatabaseInfo;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.core.loading.Capabilities;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.loading.LazyIdMapBuilder;
import org.neo4j.gds.core.loading.construction.NodeLabelTokens;
import org.neo4j.gds.core.loading.construction.PropertyValues;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.values.storable.Values;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.TestSupport.fromGdl;

class GraphImporterTest {

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void shouldImportStructure() {
        var importer = new GraphImporter(
            GraphProjectConfig.emptyWithName("", "g"),
            List.of(),
            List.of(),
            new LazyIdMapBuilder(4, true, true, PropertyState.REMOTE),
            Capabilities.WriteMode.REMOTE,
            ""
        );

        for (int i = 0; i < 2; i++) {
            importer.update(
                i,
                i + 1,
                null,
                null,
                NodeLabelTokens.empty(),
                NodeLabelTokens.empty(),
                RelationshipType.ALL_RELATIONSHIPS,
                null
            );
        }

        var result = importer.result(
            DatabaseInfo.of(DatabaseId.EMPTY, DatabaseInfo.DatabaseLocation.LOCAL),
            ProgressTimer.start(),
            true
        );

        assertThat(result.nodeCount()).isEqualTo(3);
        assertThat(result.relationshipCount()).isEqualTo(2);
        var graphStore = GraphStoreCatalog.get("", "", "g").graphStore();
        assertGraphEquals(
            fromGdl("()-->()-->()"),
            graphStore.getUnion()
        );
    }

    @Test
    void shouldImportNodesWithLabels() {
        var importer = new GraphImporter(
            GraphProjectConfig.emptyWithName("", "g"),
            List.of(),
            List.of(),
            new LazyIdMapBuilder(4, true, true, PropertyState.REMOTE),
            Capabilities.WriteMode.REMOTE,
            ""
        );

        for (int i = 0; i < 2; i++) {
            importer.update(
                i,
                i + 1,
                null,
                null,
                NodeLabelTokens.ofStrings("Label" + i),
                NodeLabelTokens.ofStrings("Label" + (i + 1)),
                RelationshipType.ALL_RELATIONSHIPS,
                null
            );
        }

        importer.result(
            DatabaseInfo.of(DatabaseId.EMPTY, DatabaseInfo.DatabaseLocation.LOCAL),
            ProgressTimer.start(),
            true
        );

        var graphStore = GraphStoreCatalog.get("", "", "g").graphStore();
        assertGraphEquals(
            fromGdl("(:Label0)-->(:Label1)-->(:Label2)"),
            graphStore.getUnion()
        );
    }

    @Test
    void shouldImportNodesWithProperties() {
        var importer = new GraphImporter(
            GraphProjectConfig.emptyWithName("", "g"),
            List.of(),
            List.of(),
            new LazyIdMapBuilder(4, true, true, PropertyState.REMOTE),
            Capabilities.WriteMode.REMOTE,
            ""
        );

        for (int i = 0; i < 2; i++) {
            importer.update(
                i,
                i + 1,
                PropertyValues.of(Map.of("prop", Values.longValue(i))),
                PropertyValues.of(Map.of("prop", Values.longValue(i + 1))),
                NodeLabelTokens.empty(),
                NodeLabelTokens.empty(),
                RelationshipType.ALL_RELATIONSHIPS,
                null
            );
        }

        importer.result(
            DatabaseInfo.of(DatabaseId.EMPTY, DatabaseInfo.DatabaseLocation.LOCAL),
            ProgressTimer.start(),
            true
        );

        var graphStore = GraphStoreCatalog.get("", "", "g").graphStore();
        assertGraphEquals(
            fromGdl("({prop: 0})-->({prop: 1})-->({prop: 2})"),
            graphStore.getUnion()
        );
    }


    @Test
    void shouldImportNodesWithPropertiesWithDifferentSchemas() {
        var importer = new GraphImporter(
            GraphProjectConfig.emptyWithName("", "g"),
            List.of(),
            List.of(),
            new LazyIdMapBuilder(4, true, true, PropertyState.REMOTE),
            Capabilities.WriteMode.REMOTE,
            ""
        );

        for (int i = 0; i < 2; i++) {
            var j = i + 1;

            importer.update(
                i,
                i + 1,
                PropertyValues.of(Map.of("prop" + i, Values.longValue(i))),
                PropertyValues.of(Map.of("prop" + j, Values.longValue(j))),
                NodeLabelTokens.ofStrings("Label" + i),
                NodeLabelTokens.ofStrings("Label" + (j)),
                RelationshipType.ALL_RELATIONSHIPS,
                null
            );
        }

        importer.result(
            DatabaseInfo.of(DatabaseId.EMPTY, DatabaseInfo.DatabaseLocation.LOCAL),
            ProgressTimer.start(),
            true
        );

        var graphStore = GraphStoreCatalog.get("", "", "g").graphStore();
        assertGraphEquals(
            fromGdl("(:Label0 {prop0: 0})-->(:Label1 {prop1: 1})-->(:Label2 {prop2: 2})"),
            graphStore.getUnion()
        );
    }

    @Test
    void shouldImportRelationshipsWithType() {
        var importer = new GraphImporter(
            GraphProjectConfig.emptyWithName("", "g"),
            List.of(),
            List.of(),
            new LazyIdMapBuilder(4, true, true, PropertyState.REMOTE),
            Capabilities.WriteMode.REMOTE,
            ""
        );

        for (int i = 0; i < 2; i++) {
            importer.update(
                i,
                i + 1,
                null,
                null,
                NodeLabelTokens.empty(),
                NodeLabelTokens.empty(),
                RelationshipType.of("REL" + i),
                null
            );
        }

        var result = importer.result(
            DatabaseInfo.of(DatabaseId.EMPTY, DatabaseInfo.DatabaseLocation.LOCAL),
            ProgressTimer.start(),
            true
        );

        assertThat(result.nodeCount()).isEqualTo(3);
        assertThat(result.relationshipCount()).isEqualTo(2);
        var graphStore = GraphStoreCatalog.get("", "", "g").graphStore();
        assertGraphEquals(
            fromGdl("()-[:REL0]->()-[:REL1]->()"),
            graphStore.getUnion()
        );
    }

    @Test
    void shouldImportRelationshipsWithProperties() {
        var importer = new GraphImporter(
            GraphProjectConfig.emptyWithName("", "g"),
            List.of(),
            List.of(),
            new LazyIdMapBuilder(4, true, true, PropertyState.REMOTE),
            Capabilities.WriteMode.REMOTE,
            ""
        );

        for (int i = 0; i < 2; i++) {
            importer.update(
                i,
                i + 1,
                null,
                null,
                NodeLabelTokens.empty(),
                NodeLabelTokens.empty(),
                RelationshipType.of("REL" + i),
                PropertyValues.of(Map.of("prop" + i, Values.longValue(i)))
            );
        }

        var result = importer.result(
            DatabaseInfo.of(DatabaseId.EMPTY, DatabaseInfo.DatabaseLocation.LOCAL),
            ProgressTimer.start(),
            true
        );

        assertThat(result.nodeCount()).isEqualTo(3);
        assertThat(result.relationshipCount()).isEqualTo(2);
        var graphStore = GraphStoreCatalog.get("", "", "g").graphStore();
        assertGraphEquals(
            fromGdl("()-[:REL0 {prop0: 0}]->()-[:REL1 {prop1: 1}]->()"),
            graphStore.getUnion()
        );
    }
}
