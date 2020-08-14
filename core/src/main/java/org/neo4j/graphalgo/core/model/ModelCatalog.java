/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.core.model;

import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public final class ModelCatalog {

    private static final Map<String, Model<?>> modelCatalog = new ConcurrentHashMap<>();

    private ModelCatalog() {}

    public static void set(Model<?> model) {
        modelCatalog.put(model.name(), model);
    }

    public static <T> Model<T> get(String modelName) {
        Model<T> model = (Model<T>) modelCatalog.get(modelName);
        if (model == null) {
            throw new IllegalArgumentException(formatWithLocale("No model with model name `%s` was found.", modelName));
        }
        return model;
    }

    public static boolean exists(String modelName) {
        return modelCatalog.containsKey(modelName);
    }

    public static Optional<String> type(String modelName) {
        return Optional.ofNullable(modelCatalog.get(modelName))
            .map(Model::algoType);
    }

    @Nullable
    public static <T> Model<T> drop(String modelName) {
        return (Model<T>) modelCatalog.remove(modelName);
    }
}
