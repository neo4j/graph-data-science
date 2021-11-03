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
import org.neo4j.gds.config.BaseConfig;
import org.neo4j.gds.config.ToMap;
import org.neo4j.gds.core.GdsEdition;
import org.neo4j.gds.model.ModelConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static org.neo4j.gds.core.StringSimilarity.prettySuggestions;

public final class OpenModelCatalog implements ModelCatalog {

    public static final ModelCatalog INSTANCE = new OpenModelCatalog();

    private OpenModelCatalog() {}

    private static final Map<String, OpenUserCatalog> userCatalogs = new ConcurrentHashMap<>();
    private static final OpenUserCatalog publicModels = new OpenUserCatalog();

    @Override
    public void set(Model<?, ?, ?> model) {
        userCatalogs.compute(model.creator(), (user, userCatalog) -> {
            if (userCatalog == null) {
                userCatalog = new OpenUserCatalog();
            }
            userCatalog.set(model);
            return userCatalog;
        });
    }

    @Override
    public void setUnsafe(Model<?, ?, ?> model) {
        userCatalogs.compute(model.creator(), (user, userCatalog) -> {
            if (userCatalog == null) {
                userCatalog = new OpenUserCatalog();
            }
            userCatalog.setUnsafe(model);
            return userCatalog;
        });
    }

    @Override
    public <D, C extends ModelConfig, I extends ToMap> Model<D, C, I> get(
        String username,
        String modelName,
        Class<D> dataClass,
        Class<C> configClass,
        Class<I> infoClass
    ) {
        var userCatalog = getUserCatalog(username);
        var userModel = userCatalog.get(modelName, dataClass, configClass, infoClass);
        if (userModel != null) {
            return userModel;
        } else {
            var publicModel = publicModels.get(modelName, dataClass, configClass, infoClass);
            if (publicModel != null) {
                return publicModel;
            }
        }
        throw new NoSuchElementException(prettySuggestions(
            formatWithLocale("Model with name `%s` does not exist.", modelName),
            modelName,
            userCatalog.availableModelNames()
        ));
    }

    @Override
    public @Nullable Model<?, ?, ?> getUntyped(String username, String modelName) {
        return getUntyped(username, modelName, true);
    }

    @Override
    public @Nullable Model<?, ?, ?> getUntyped(String username, String modelName, boolean failOnMissing) {
        var userCatalog = getUserCatalog(username);
        var model = userCatalog.getUntyped(modelName);
        if (model == null) {
            model = publicModels.getUntyped(modelName);
        }

        if (model == null && failOnMissing) {
            throw new NoSuchElementException(prettySuggestions(
                formatWithLocale("Model with name `%s` does not exist.", modelName),
                modelName,
                userCatalog.availableModelNames()
            ));
        }

        return model;
    }

    @Override
    public Stream<Model<?, ?, ?>> getAllModels() {
        return userCatalogs
            .entrySet()
            .stream()
            .flatMap(entry -> entry.getValue().streamModels());
    }

    @Override
    public boolean exists(String username, String modelName) {
        return getUserCatalog(username).exists(modelName) ||
               publicModels.exists(modelName);
    }

    @Override
    public Optional<String> type(String username, String modelName) {
        return getUserCatalog(username).type(modelName);
    }

    @Override
    public Model<?, ?, ?> drop(String username, String modelName) {
        return drop(username, modelName, true);
    }

    @Override
    public Model<?, ?, ?> drop(String username, String modelName, boolean failOnMissing) {
        if (publicModels.exists(modelName)) {
            var model = publicModels.getUntyped(modelName);
            if (model.creator().equals(username)) {
                return publicModels.drop(modelName, failOnMissing);
            }
            throw new IllegalStateException(formatWithLocale("Only the creator of model %s can drop it.", modelName));
        } else {
            return getUserCatalog(username).drop(modelName, failOnMissing);
        }
    }

    @Override
    public Collection<Model<?, ?, ?>> list(String username) {
        var models = new ArrayList<>(getUserCatalog(username).list());
        models.addAll(publicModels.list());
        return models;
    }

    @Override
    public @Nullable Model<?, ?, ?> list(String username, String modelName) {
        return getUntyped(username, modelName, false);
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
        publicModels.removeAllLoadedModels();
    }

    @Override
    public void checkStorable(String username, String modelName, String modelType) {
        getUserCatalog(username).checkStorable(modelName, modelType);
    }

    private static OpenUserCatalog getUserCatalog(String username) {
        return userCatalogs.getOrDefault(username, new OpenUserCatalog());
    }

    private static String formatWithLocale(String template, Object... inputs) {
        return String.format(Locale.ENGLISH, template, inputs);
    }
}
