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
package org.neo4j.gds.internal;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.neo4j.gds.QueryRunner;
import org.neo4j.gds.embeddings.graphsage.EmptyGraphSageTrainMetrics;
import org.neo4j.gds.embeddings.graphsage.Layer;
import org.neo4j.gds.embeddings.graphsage.ModelData;
import org.neo4j.gds.embeddings.graphsage.SingleLabelFeatureFunction;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSage;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.gds.model.storage.ModelToFileExporter;
import org.neo4j.graphalgo.NodeProjection;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.graphalgo.gdl.ImmutableGraphCreateFromGdlConfig;
import org.neo4j.gds.utils.ExceptionUtil;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

final class AuraTestSupport {

    private static final String DB_CYPHER = "CREATE" +
                                            "  (a:Label1 {prop1: 42})" +
                                            ", (b:Label1)" +
                                            ", (c:Label2 {prop2: 1337})" +
                                            ", (d:Label2 {prop2: 10})" +
                                            ", (e:Label2)" +
                                            ", (a)-[:REL1]->(b)" +
                                            ", (c)-[:REL2]->(d)";

    private AuraTestSupport() {}

    static void setupGraphsAndModels(GraphDatabaseAPI db) {
        QueryRunner.runQuery(db, DB_CYPHER);

        var graphStore1 = new StoreLoaderBuilder()
            .api(db)
            .addNodeProjection(NodeProjection.of("Label1", PropertyMappings.of(PropertyMapping.of("prop1"))))
            .addRelationshipProjection(RelationshipProjection.of("REL1", Orientation.NATURAL))
            .build()
            .graphStore();
        var createConfig1 = ImmutableGraphCreateFromGdlConfig
            .builder()
            .gdlGraph("")
            .graphName("first")
            .username("userA")
            .build();
        GraphStoreCatalog.set(createConfig1, graphStore1);

        var graphStore2 = new StoreLoaderBuilder()
            .api(db)
            .addNodeProjection(NodeProjection.of("Label2", PropertyMappings.of(PropertyMapping.of("prop2"))))
            .addRelationshipProjection(RelationshipProjection.of("REL2", Orientation.NATURAL))
            .build()
            .graphStore();
        var createConfig2 = ImmutableGraphCreateFromGdlConfig
            .builder()
            .gdlGraph("")
            .graphName("second")
            .username("userB")
            .build();
        GraphStoreCatalog.set(createConfig2, graphStore2);

        var model1 = Model.of(
            "userA",
            "firstModel",
            GraphSage.MODEL_TYPE,
            GraphSchema.empty(),
            ModelData.of(new Layer[]{}, new SingleLabelFeatureFunction()),
            GraphSageTrainConfig.builder().modelName("firstModel").addFeatureProperty("foo").build(),
            EmptyGraphSageTrainMetrics.INSTANCE
        );
        ModelCatalog.set(model1);

        var model2 = Model.of(
            "userB",
            "secondModel",
            GraphSage.MODEL_TYPE,
            GraphSchema.empty(),
            ModelData.of(new Layer[]{}, new SingleLabelFeatureFunction()),
            GraphSageTrainConfig.builder().modelName("secondModel").addFeatureProperty("foo").build(),
            EmptyGraphSageTrainMetrics.INSTANCE
        );
        ModelCatalog.set(model2);
    }

    static void assertGraph(String path) {
        assertGraph(Paths.get(path));
    }

    static void assertGraph(Path path) {
        assertThat(path)
            .isDirectoryContaining("glob:**/.userinfo")
            .isDirectoryContaining("glob:**/graph_info.csv")
            .isDirectoryContaining("glob:**/node-schema.csv")
            .isDirectoryContaining("glob:**/relationship-schema.csv")
            .isDirectoryContaining("regex:.+/nodes_Label[12]_header\\.csv")
            .isDirectoryContaining("regex:.+/nodes_Label[12]_\\d+\\.csv")
            .isDirectoryContaining("regex:.+/relationships_REL[12]_header\\.csv")
            .isDirectoryContaining("regex:.+/relationships_REL[12]_\\d+\\.csv");
    }

    static void assertGraphs(Path root) {
        assertGraph(root.resolve("userA/graphs/first"));
        assertThat(root.resolve("userA/graphs/second")).doesNotExist();
        assertGraph(root.resolve("userB/graphs/second"));
        assertThat(root.resolve("userB/graphs/first")).doesNotExist();
    }

    static void assertModel(String path) {
        assertModel(Paths.get(path));
    }

    static void assertModel(Path path) {
        assertThat(path)
            .satisfies(p -> assertThat(ExceptionUtil.apply(Files::list, p).count()).isEqualTo(2))
            .isDirectoryContaining("glob:**/" + ModelToFileExporter.META_DATA_FILE)
            .isDirectoryContaining("glob:**/" + ModelToFileExporter.MODEL_DATA_FILE);
    }

    static void assertModels(Path root) {
        assertBackupModels(root.resolve("userA/models"));
        assertBackupModels(root.resolve("userB/models"));
    }

    private static void assertBackupModels(Path path) {
        assertThat(ExceptionUtil.apply(Files::list, path).collect(Collectors.toList()))
            .hasSize(1)
            .element(0, InstanceOfAssertFactories.PATH)
            .satisfies(AuraTestSupport::assertModel);
    }
}

