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
package org.neo4j.gds.model;

import org.neo4j.gds.embeddings.graphsage.EmptyGraphSageTrainMetrics;
import org.neo4j.gds.embeddings.graphsage.Layer;
import org.neo4j.gds.embeddings.graphsage.ModelData;
import org.neo4j.gds.embeddings.graphsage.SingleLabelFeatureFunction;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSage;
import org.neo4j.gds.embeddings.graphsage.algo.ImmutableGraphSageTrainConfig;
import org.neo4j.gds.model.storage.ModelToFileExporter;
import org.neo4j.graphalgo.api.schema.GraphSchema;
import org.neo4j.graphalgo.core.model.Model;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

public final class ModelStoreUtil {

    public static Path createAndStoreModel(
        Path storeDir,
        String modelName,
        String userName
    ) throws IOException {
        var trainConfig = ImmutableGraphSageTrainConfig.builder()
            .modelName(modelName)
            .addFeatureProperties("a")
            .relationshipWeightProperty("weight")
            .concurrency(1)
            .build();

        var modelData = ModelData.of(new Layer[]{}, new SingleLabelFeatureFunction());

        var model = Model.of(
            userName,
            modelName,
            GraphSage.MODEL_TYPE,
            GraphSchema.empty(),
            modelData,
            trainConfig,
            EmptyGraphSageTrainMetrics.INSTANCE
        );

        var resolvedStoreDir = storeDir.resolve(UUID.randomUUID().toString());
        Files.createDirectory(resolvedStoreDir);

        ModelToFileExporter.toFile(resolvedStoreDir, model);

        return resolvedStoreDir;
    }

    private ModelStoreUtil() {}
}
