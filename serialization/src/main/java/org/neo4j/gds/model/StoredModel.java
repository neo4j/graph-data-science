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

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.model.storage.ModelFileReader;
import org.neo4j.gds.model.storage.ModelFileWriter;
import org.neo4j.gds.model.storage.ModelInfoSerializerFactory;
import org.neo4j.gds.model.storage.TrainConfigSerializerFactory;
import org.neo4j.graphalgo.api.schema.GraphSchema;
import org.neo4j.graphalgo.api.schema.SchemaDeserializer;
import org.neo4j.graphalgo.config.ModelConfig;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.model.ModelMetaDataSerializer;
import org.neo4j.graphalgo.core.model.ZonedDateTimeSerializer;
import org.neo4j.graphalgo.core.model.proto.ModelProto;

import java.io.IOException;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.List;

import static org.neo4j.gds.model.storage.ModelToFileExporter.META_DATA_FILE;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class StoredModel implements Model<Object, ModelConfig> {

    private final ModelProto.ModelMetaData metaData;
    private final ModelFileReader modelReader;
    private final Path fileLocation;

    @Nullable
    private Object data;
    private boolean loaded;

    public static StoredModel withInitialData(Path storeDir, Object data) throws IOException {
        var storedModel = new StoredModel(storeDir);
        storedModel.setData(data);
        return storedModel;
    }

    public StoredModel(Path storeDir) throws IOException {
        this(storeDir, new ModelFileReader(storeDir).readMetaData(), false);
    }

    private StoredModel(Path storeDir, ModelProto.ModelMetaData metaData, boolean loaded) throws IOException {
        this.modelReader = new ModelFileReader(storeDir);
        this.metaData = metaData;
        this.loaded = loaded;
        this.fileLocation = storeDir;
    }

    @Override
    public void load() {
        if (loaded) {
            return;
        }

        try {
            this.data = modelReader.readData(algoType());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.loaded = true;
    }

    @Override
    public void unload() {
        this.data = null;
        this.loaded = false;
    }

    @Override
    public boolean loaded() {
        return loaded;
    }

    @Override
    public boolean stored() {
        return true;
    }

    @Override
    public String creator() {
        return metaData.getCreator();
    }

    @Override
    public String name() {
        return metaData.getName();
    }

    @Override
    public String algoType() {
        return metaData.getAlgoType();
    }

    @Override
    public GraphSchema graphSchema() {
        return SchemaDeserializer.graphSchema(metaData.getGraphSchema());
    }

    @Override
    public List<String> sharedWith() {
        return metaData.getSharedWithList();
    }

    @Override
    public Model<Object, ModelConfig> publish() {
        try {
            var deletedMetaData = fileLocation.resolve(META_DATA_FILE).toFile().delete();

            if (deletedMetaData) {
                ModelProto.ModelMetaData publishedMetaData = ModelMetaDataSerializer.toPublishable(
                    metaData,
                    name() + PUBLIC_MODEL_SUFFIX,
                    List.of(Model.ALL_USERS)
                );


                StoredModel publishedModel = new StoredModel(fileLocation, publishedMetaData, loaded);

                new ModelFileWriter<>(
                    fileLocation,
                    publishedModel
                ).writeMetaData();
                return publishedModel;
            } else {
                throw new RuntimeException(formatWithLocale("Could not publish %s.", name()));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ModelConfig trainConfig() {
        return ModelSupport.onValidAlgoType(
            algoType(),
            () -> TrainConfigSerializerFactory.trainConfigSerializer(algoType()).fromSerializable(metaData.getTrainConfig())
        );
    }

    @Override
    public ZonedDateTime creationTime() {
        return ZonedDateTimeSerializer.fromSerializable(metaData.getCreationTime());
    }

    @Override
    public Mappable customInfo() {
        return ModelSupport.onValidAlgoType(
            algoType(),
            () -> ModelInfoSerializerFactory.modelInfoSerializer(algoType()).fromSerializable(metaData.getCustomInfo())
        );
    }

    @Override
    public Object data() {
        if (!loaded) {
            throw new IllegalStateException(formatWithLocale(
                "The model '%s' is currently not loaded.",
                name()
            ));
        }

        return data;
    }

    public void setData(Object inputData) {
        loaded = true;
        data = inputData;
    }

    public Path fileLocation() {
        return fileLocation;
    }
}
