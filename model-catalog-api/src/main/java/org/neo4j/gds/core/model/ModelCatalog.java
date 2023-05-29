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
import org.neo4j.gds.config.ToMapConvertible;
import org.neo4j.gds.model.ModelConfig;

import java.util.Collection;
import java.util.stream.Stream;

public interface ModelCatalog {

    void registerListener(ModelCatalogListener listener);

    void unregisterListener(ModelCatalogListener listener);

    void set(Model<?, ?, ?> model);

    <D, C extends ModelConfig, I extends ToMapConvertible> Model<D, C, I> get(
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

    boolean isEmpty();

    void removeAllLoadedModels();

    void verifyModelCanBeStored(String username, String modelName, String modelType);
}
