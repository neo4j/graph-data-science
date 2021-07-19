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

import org.junit.jupiter.api.BeforeEach;
import org.neo4j.gds.embeddings.graphsage.EmptyGraphSageTrainMetrics;
import org.neo4j.gds.embeddings.graphsage.Layer;
import org.neo4j.gds.embeddings.graphsage.ModelData;
import org.neo4j.gds.embeddings.graphsage.SingleLabelFeatureFunction;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSage;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.NodeProjection;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.TestLog;
import org.neo4j.graphalgo.api.schema.GraphSchema;
import org.neo4j.graphalgo.compat.GraphDatabaseApiProxy;
import org.neo4j.graphalgo.compat.GraphStoreExportSettings;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.graphalgo.gdl.ImmutableGraphCreateFromGdlConfig;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;

import java.nio.file.Path;

public abstract class AuraBackupBaseProcTest extends BaseProcTest {

    private static final String DB_CYPHER = "CREATE" +
                                            "  (a:Label1 {prop1: 42})" +
                                            ", (b:Label1)" +
                                            ", (c:Label2 {prop2: 1337})" +
                                            ", (d:Label2 {prop2: 10})" +
                                            ", (e:Label2)" +
                                            ", (a)-[:REL1]->(b)" +
                                            ", (c)-[:REL2]->(d)";

    TestLog testLog;

    @BeforeEach
    void setup() throws Exception {
        GraphDatabaseApiProxy.resolveDependency(db, GlobalProcedures.class).register(new AuraBackupProc());

        runQuery(DB_CYPHER);

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

    @Override
    @ExtensionCallback
    protected void configuration(TestDatabaseManagementServiceBuilder builder) {
        super.configuration(builder);
        testLog = new TestLog();
        builder.setUserLogProvider(new LogProvider() {
            @Override
            public Log getLog(Class<?> loggingClass) {
                return testLog;
            }

            @Override
            public Log getLog(String name) {
                return testLog;
            }
        });
        builder.setConfig(GraphStoreExportSettings.backup_location_setting, getBackupLocation());
    }

    abstract Path getBackupLocation();
}
