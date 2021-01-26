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
import org.neo4j.graphalgo.api.schema.GraphSchema;
import org.neo4j.graphalgo.api.schema.SchemaDeserializer;
import org.neo4j.graphalgo.config.GraphSageTrainConfigSerializer;
import org.neo4j.graphalgo.config.ModelConfig;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.model.ZonedDateTimeSerializer;
import org.neo4j.graphalgo.core.model.proto.ModelProto;
import org.neo4j.io.fs.FileUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.List;

import static org.neo4j.gds.model.storage.ModelToFileExporter.META_DATA_FILE;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public class PersistedModel implements Model<Object, ModelConfig> {

    private final ModelProto.ModelMetaData metaData;
    private final ModelFileReader modelReader;
    private final Path fileLocation;

    @Nullable
    private Object data;
    private boolean loaded;

    public PersistedModel(Path persistenceDir) throws IOException {
        this(persistenceDir, new ModelFileReader(persistenceDir).readMetaData(), false);
    }

    private PersistedModel(Path persistenceDir, ModelProto.ModelMetaData metaData, boolean loaded) throws IOException {
        this.modelReader = new ModelFileReader(persistenceDir);
        this.metaData = metaData;
        this.loaded = loaded;
        this.fileLocation = persistenceDir;
    }

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

    public void unload() {
        this.data = null;
        this.loaded = false;
    }

    @Override
    public boolean loaded() {
        return loaded;
    }

    @Override
    public boolean persisted() {
        return true;
    }

    @Override
    public String username() {
        return metaData.getUsername();
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
        ModelProto.ModelMetaData publishedMetaData = ModelProto.ModelMetaData.newBuilder(metaData)
            .setName(name() + "_public")
            .addAllSharedWith(List.of(Model.ALL_USERS))
            .build();

        try {
            FileUtils.delete(fileLocation.resolve(META_DATA_FILE));

            PersistedModel publishedModel = new PersistedModel(fileLocation, publishedMetaData, loaded);

            new ModelFileWriter<>(
                fileLocation,
                publishedModel
            ).writeMetaData();
            return publishedModel;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ModelConfig trainConfig() {
        return ModelSupport.onValidAlgoType(
            algoType(),
            () -> GraphSageTrainConfigSerializer.fromSerializable(metaData.getGraphSageTrainConfig())
        );
    }

    @Override
    public ZonedDateTime creationTime() {
        return ZonedDateTimeSerializer.fromSerializable(metaData.getCreationTime());
    }

    @Override
    public Mappable customInfo() {
        return Mappable.EMPTY;
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

    public Path fileLocation() {
        return fileLocation;
    }
}
