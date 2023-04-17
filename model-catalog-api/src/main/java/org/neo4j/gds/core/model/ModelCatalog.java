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
package org.neo4j.gds.core.model;

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.core.model.Model.CustomInfo;
import org.neo4j.gds.model.ModelConfig;
import org.neo4j.graphdb.GraphDatabaseService;

import java.nio.file.Path;
import java.util.Collection;
import java.util.stream.Stream;

public interface ModelCatalog {

    void registerListener(ModelCatalogListener listener);

    void set(Model<?, ?, ?> model);

    <D, C extends ModelConfig, I extends CustomInfo> Model<D, C, I> get(
        String username,
        String modelName,
        Class<D> dataClass,
        Class<C> configClass,
        Class<I> infoClass
    );

    Model<?, ?, ?> getUntypedOrThrow(String username, String modelName);

    @Nullable Model<?, ?, ?> getUntyped(String username, String modelName);

    Stream<Model<?, ?, ?>> getAllModels();

    long modelCount();

    boolean exists(String username, String modelName);

    Model<?, ?, ?> dropOrThrow(String username, String modelName);

    @Nullable Model<?, ?, ?> drop(String username, String modelName);

    Collection<Model<?, ?, ?>> list(String username);

    Model<?, ?, ?> publish(String username, String modelName);

    void checkLicenseBeforeStoreModel(GraphDatabaseService db, String detail);

    Path getModelDirectory(GraphDatabaseService db);

    Model<?, ?, ?> store(String username, String modelName, Path modelDir);

    boolean isEmpty();

    void removeAllLoadedModels();

    void verifyModelCanBeStored(String username, String modelName, String modelType);

    ModelCatalog EMPTY = new ModelCatalog() {
        @Override
        public void registerListener(ModelCatalogListener listener) {

        }

        @Override
        public void set(Model<?, ?, ?> model) {

        }

        @Override
        public <D, C extends ModelConfig, I extends CustomInfo> Model<D, C, I> get(
            String username,
            String modelName,
            Class<D> dataClass,
            Class<C> configClass,
            Class<I> infoClass
        ) {
            return null;
        }

        @Override
        public Model<?, ?, ?> getUntypedOrThrow(String username, String modelName) {
            return null;
        }

        @Override
        public @Nullable Model<?, ?, ?> getUntyped(String username, String modelName) {
            return null;
        }

        @Override
        public Stream<Model<?, ?, ?>> getAllModels() {
            return null;
        }

        @Override
        public long modelCount() {
            return 0;
        }

        @Override
        public boolean exists(String username, String modelName) {
            return false;
        }

        @Override
        public Model<?, ?, ?> dropOrThrow(String username, String modelName) {
            return null;
        }

        @Override
        public @Nullable Model<?, ?, ?> drop(String username, String modelName) {
            return null;
        }

        @Override
        public Collection<Model<?, ?, ?>> list(String username) {
            return null;
        }

        @Override
        public Model<?, ?, ?> publish(String username, String modelName) {
            return null;
        }

        @Override
        public void checkLicenseBeforeStoreModel(GraphDatabaseService db, String detail) { }

        @Override
        public Path getModelDirectory(GraphDatabaseService db) { return null; }

        @Override
        public Model<?, ?, ?> store(String username, String modelName, Path modelDir) { return null; }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public void removeAllLoadedModels() {

        }

        @Override
        public void verifyModelCanBeStored(String username, String modelName, String modelType) {

        }
    };
}
