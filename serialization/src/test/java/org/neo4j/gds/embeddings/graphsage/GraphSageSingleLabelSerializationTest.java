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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.model.ModelMetaDataSerializer;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSage;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.gds.embeddings.graphsage.algo.ImmutableGraphSageStreamConfig;
import org.neo4j.gds.embeddings.graphsage.algo.ImmutableGraphSageTrainConfig;
import org.neo4j.gds.embeddings.graphsage.algo.SingleLabelGraphSageTrain;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.graphalgo.core.model.proto.ModelProto;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.progress.v2.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.utils.SerializationUtil.serializationRoundTrip;

@GdlExtension
class GraphSageSingleLabelSerializationTest {

    @GdlGraph
    private static final String GRAPH =
        "CREATE" +
        "  (a:King{ age: 20, birth_year: 200, death_year: 300 })" +
        ", (b:King{ age: 12, birth_year: 232, death_year: 300 })" +
        ", (c:King{ age: 67, birth_year: 212, death_year: 300 })" +
        ", (d:King{ age: 78, birth_year: 245, death_year: 300 })" +
        ", (e:King{ age: 32, birth_year: 256, death_year: 300 })" +
        ", (f:King{ age: 32, birth_year: 214, death_year: 300 })" +
        ", (g:King{ age: 35, birth_year: 214, death_year: 300 })" +
        ", (h:King{ age: 56, birth_year: 253, death_year: 300 })" +
        ", (i:King{ age: 62, birth_year: 267, death_year: 300 })" +
        ", (j:King{ age: 44, birth_year: 289, death_year: 300 })" +
        ", (k:King{ age: 89, birth_year: 211, death_year: 300 })" +
        ", (l:King{ age: 99, birth_year: 201, death_year: 300 })" +
        ", (m:King{ age: 99, birth_year: 201, death_year: 300 })" +
        ", (n:King{ age: 99, birth_year: 201, death_year: 300 })" +
        ", (o:King{ age: 99, birth_year: 201, death_year: 300 })" +
        ", (a)-[:REL]->(b)" +
        ", (a)-[:REL]->(c)" +
        ", (b)-[:REL]->(c)" +
        ", (b)-[:REL]->(d)" +
        ", (c)-[:REL]->(e)" +
        ", (d)-[:REL]->(e)" +
        ", (d)-[:REL]->(f)" +
        ", (e)-[:REL]->(f)" +
        ", (e)-[:REL]->(g)" +
        ", (h)-[:REL]->(i)" +
        ", (i)-[:REL]->(j)" +
        ", (j)-[:REL]->(k)" +
        ", (j)-[:REL]->(l)" +
        ", (k)-[:REL]->(l)";
    public static final String MODEL_NAME = "e2e";

    @Inject
    private Graph graph;

    @Test
    void e2eTest() throws IOException {
        var model = train();
        var originalEmbeddings = produceEmbeddings(model);

        var protoModelMetaData = serializationRoundTrip(
            ModelMetaDataSerializer.toSerializable(model),
            ModelProto.ModelMetaData.parser()
        );
        var serializer = new GraphSageModelSerializer();
        var protoGraphSageModel = serializationRoundTrip(
            serializer.toSerializable(model.data()),
            serializer.modelParser()
        );

        var deserializedModel = serializer.fromSerializable(
            protoGraphSageModel,
            protoModelMetaData
        );
        var embeddingsFromDeserializedModel = produceEmbeddings(deserializedModel);

        assertThat(originalEmbeddings)
            .usingRecursiveComparison()
            .withStrictTypeChecking()
            .isEqualTo(embeddingsFromDeserializedModel);
    }

    private GraphSage.GraphSageResult produceEmbeddings(Model<ModelData, GraphSageTrainConfig> model) {
        ModelCatalog.drop("", model.name(), false);
        ModelCatalog.set(model);

        var streamConfig = ImmutableGraphSageStreamConfig
            .builder()
            .modelName(model.name())
            .build();

        return new GraphSage(
            graph,
            streamConfig,
            Pools.DEFAULT,
            AllocationTracker.empty(),
            ProgressTracker.NULL_TRACKER
        ).compute();
    }

    private Model<ModelData, GraphSageTrainConfig> train() {
        var trainConfig = ImmutableGraphSageTrainConfig.builder()
            .modelName(MODEL_NAME)
            .featureProperties(List.of("age", "birth_year", "death_year"))
            .build();

        SingleLabelGraphSageTrain trainAlgo = new SingleLabelGraphSageTrain(
            graph,
            trainConfig,
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER,
            AllocationTracker.empty()
        );

        return trainAlgo.compute();
    }
}
