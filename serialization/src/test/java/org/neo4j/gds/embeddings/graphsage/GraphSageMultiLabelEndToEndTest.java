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
package org.neo4j.gds.embeddings.graphsage;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSage;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.gds.embeddings.graphsage.algo.ImmutableGraphSageStreamConfig;
import org.neo4j.gds.embeddings.graphsage.algo.ImmutableGraphSageTrainConfig;
import org.neo4j.gds.embeddings.graphsage.algo.MultiLabelGraphSageTrain;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.model.ModelMetaDataSerializer;
import org.neo4j.graphalgo.core.model.proto.GraphSageProto;
import org.neo4j.graphalgo.core.model.proto.ModelProto;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.embeddings.graphsage.GraphSageTestGraph;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.graphalgo.utils.SerializationUtil.serializationRoundTrip;

@GdlExtension
class GraphSageMultiLabelEndToEndTest {

    @SuppressFBWarnings("HSC_HUGE_SHARED_STRING_CONSTANT")
    @GdlGraph
    private static final String GRAPH = GraphSageTestGraph.GDL;

    private static final String MODEL_NAME = "e2e";

    @Inject
    private Graph graph;

    @Test
    void e2eTest() throws IOException, ClassNotFoundException {
        Model<ModelData, GraphSageTrainConfig> model = train();
        var originalEmbeddings = produceEmbeddings(model);

        var protoModelMetaData = serializationRoundTrip(
            ModelMetaDataSerializer.toSerializable(model),
            ModelProto.ModelMetaData.parser()
        );
        var serializer = new GraphSageModelSerializer();
        var protoGraphSageModel = serializationRoundTrip(
            serializer.toSerializable(model.data()),
            GraphSageProto.GraphSageModel.parser()
        );

        Model<ModelData, GraphSageTrainConfig> deserializedModel = serializer.fromSerializable(protoGraphSageModel, protoModelMetaData);

        assertThat(deserializedModel.data().layers())
            .isNotNull()
            .usingRecursiveComparison()
            .withStrictTypeChecking()
            .ignoringFieldsOfTypes(Random.class)
            .isEqualTo(model.data().layers());

        assertThat(deserializedModel.data().featureFunction())
            .isNotNull()
            .usingRecursiveComparison()
            .isEqualTo(model.data().featureFunction());

        assertThat(originalEmbeddings)
            .usingRecursiveComparison()
            .withStrictTypeChecking()
            .isEqualTo(produceEmbeddings(deserializedModel));
    }

    private GraphSage.GraphSageResult produceEmbeddings(Model<ModelData, GraphSageTrainConfig> model) {
        var streamConfig = ImmutableGraphSageStreamConfig
            .builder()
            .modelName(model.name())
            .build();

        return new GraphSage(
            graph,
            streamConfig,
            model,
            Pools.DEFAULT, AllocationTracker.empty(),
            ProgressLogger.NULL_LOGGER
        ).compute();
    }

    private Model<ModelData, GraphSageTrainConfig> train() {
        var trainConfig = ImmutableGraphSageTrainConfig.builder()
            .featureProperties(List.of("numEmployees", "numIngredients", "rating", "numPurchases", "embedding"))
            .embeddingDimension(64)
            .modelName(MODEL_NAME)
            .degreeAsProperty(true)
            .projectedFeatureDimension(5)
            .build();

        var trainAlgo = new MultiLabelGraphSageTrain(
            graph,
            trainConfig,
            Pools.DEFAULT, ProgressLogger.NULL_LOGGER,
            AllocationTracker.empty()
        );

        return trainAlgo.compute();
    }
}
