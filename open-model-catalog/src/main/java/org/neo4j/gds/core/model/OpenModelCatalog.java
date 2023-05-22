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

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.config.ToMapConvertible;
import org.neo4j.gds.model.ModelConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static org.neo4j.gds.core.StringSimilarity.prettySuggestions;

public final class OpenModelCatalog implements ModelCatalog {

    private final Map<String, OpenUserCatalog> userCatalogs;

    public OpenModelCatalog() {
        this.userCatalogs = new ConcurrentHashMap<>();
    }

    @Override
    public void set(Model<?, ?, ?> model) {
        userCatalogs.compute(model.creator(), (user, userCatalog) -> {
            if (userCatalog == null) {
                userCatalog = new OpenUserCatalog();
            }
            userCatalog.set(model);
            return userCatalog;
        });

        LISTENERS.forEach(listener -> listener.onInsert(model));
    }

    @Override
    public <D, C extends ModelConfig, I extends ToMapConvertible> Model<D, C, I> get(
        String username,
        String modelName,
        Class<D> dataClass,
        Class<C> configClass,
        Class<I> infoClass
    ) {
        var userCatalog = getUserCatalog(username);
        var model = userCatalog.get(modelName, dataClass, configClass, infoClass);
        if (model == null) {
            throw new NoSuchElementException(prettySuggestions(
                formatWithLocale("Model with name `%s` does not exist.", modelName),
                modelName,
                userCatalog.availableModelNames()
            ));
        }

        return model;
    }

    @Override
    public Model<?, ?, ?> getUntypedOrThrow(String username, String modelName) {
        return getUntyped(username, modelName, true);
    }

    @Override
    public @Nullable Model<?, ?, ?> getUntyped(String username, String modelName) {
        return getUntyped(username, modelName, false);
    }

    @Override
    public Stream<Model<?, ?, ?>> getAllModels() {
        return userCatalogs
            .entrySet()
            .stream()
            .flatMap(entry -> entry.getValue().streamModels());
    }

    @Override
    public long modelCount() {
        return userCatalogs.values().stream().mapToLong(OpenUserCatalog::size).sum();
    }

    @Override
    public boolean exists(String username, String modelName) {
        return getUserCatalog(username).exists(modelName);
    }

    @Override
    public Model<?, ?, ?> dropOrThrow(String username, String modelName) {
        return drop(username, modelName, true);
    }

    @Override
    public @Nullable Model<?, ?, ?> drop(String username, String modelName) {
        return drop(username, modelName, false);
    }

    @Override
    public Collection<Model<?, ?, ?>> list(String username) {
        return new ArrayList<>(getUserCatalog(username).list());
    }

    @Override
    public Model<?, ?, ?> publish(String username, String modelName) {
        throw new IllegalStateException(
            "Publishing models is not available in openGDS. " +
            "Please consider licensing the Graph Data Science library. " +
            "See documentation at https://neo4j.com/docs/graph-data-science/"
        );
    }

    @Override
    public boolean isEmpty() {
        return userCatalogs
            .values()
            .stream()
            .allMatch(userCatalog -> userCatalog.list().isEmpty());
    }

    @Override
    public void removeAllLoadedModels() {
        userCatalogs.clear();
    }

    @Override
    public void verifyModelCanBeStored(String username, String modelName, String modelType) {
        getUserCatalog(username).verifyModelCanBeStored(modelName, modelType);
    }

    @Contract(value = "_, _, true -> !null")
    private @Nullable Model<?, ?, ?> getUntyped(String username, String modelName, boolean failOnMissing) {
        var userCatalog = getUserCatalog(username);
        var model = userCatalog.getUntyped(modelName);
        if (model == null && failOnMissing) {
            throw new NoSuchElementException(prettySuggestions(
                formatWithLocale("Model with name `%s` does not exist.", modelName),
                modelName,
                userCatalog.availableModelNames()
            ));
        }

        return model;
    }

    @Contract(value = "_, _, true -> !null")
    private @Nullable Model<?, ?, ?> drop(String username, String modelName, boolean failOnMissing) {
        return getUserCatalog(username).drop(modelName, failOnMissing);
    }

    private OpenUserCatalog getUserCatalog(String username) {
        return userCatalogs.getOrDefault(username, new OpenUserCatalog());
    }

    private static String formatWithLocale(String template, Object... inputs) {
        return String.format(Locale.ENGLISH, template, inputs);
    }
}
