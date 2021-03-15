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
package org.neo4j.graphalgo.core.model;

import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.config.BaseConfig;
import org.neo4j.graphalgo.config.ModelConfig;
import org.neo4j.graphalgo.core.GdsEdition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static org.neo4j.graphalgo.core.StringSimilarity.prettySuggestions;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public final class ModelCatalog {

    private ModelCatalog() {}

    private static final Map<String, UserCatalog> userCatalogs = new ConcurrentHashMap<>();
    private static final UserCatalog publicModels = new UserCatalog();

    public static void set(Model<?, ?> model) {
        userCatalogs.compute(model.creator(), (user, userCatalog) -> {
            if (userCatalog == null) {
                userCatalog = new UserCatalog();
            }
            userCatalog.set(model);
            return userCatalog;
        });
    }

    public static void setUnsafe(Model<?, ?> model) {
        userCatalogs.compute(model.creator(), (user, userCatalog) -> {
            if (userCatalog == null) {
                userCatalog = new UserCatalog();
            }
            userCatalog.setUnsafe(model);
            return userCatalog;
        });
    }

    public static <D, C extends ModelConfig & BaseConfig> Model<D, C> get(
        String username, String modelName, Class<D> dataClass, Class<C> configClass
    ) {
        var userCatalog = getUserCatalog(username);
        var userModel = userCatalog.get(modelName, dataClass, configClass);
        if (userModel != null) {
            return userModel;
        } else {
            var publicModel = publicModels.get(modelName, dataClass, configClass);
            if (publicModel != null) {
                return publicModel;
            }
        }
        throw new NoSuchElementException(prettySuggestions(
            formatWithLocale("Model with name `%s` does not exist.", modelName),
            modelName,
            userCatalog.userModels.keySet()
        ));
    }

    public static @Nullable Model<?, ?> getUntyped(String username, String modelName) {
        return getUntyped(username, modelName, true);
    }

    public static @Nullable Model<?, ?> getUntyped(String username, String modelName, boolean failOnMissing) {
        var userCatalog = getUserCatalog(username);
        var model = userCatalog.getUntyped(modelName);
        if (model == null) {
            model = publicModels.getUntyped(modelName);
        }

        if (model == null && failOnMissing) {
            throw new NoSuchElementException(prettySuggestions(
                formatWithLocale("Model with name `%s` does not exist.", modelName),
                modelName,
                userCatalog.userModels.keySet()
            ));
        }

        return model;
    }

    public static boolean exists(String username, String modelName) {
        return getUserCatalog(username).exists(modelName) ||
               publicModels.exists(modelName);
    }

    public static Optional<String> type(String username, String modelName) {
        return getUserCatalog(username).type(modelName);
    }

    public static Model<?, ?> drop(String username, String modelName) {
        if (publicModels.exists(modelName)) {
            Model<?, ?> model = publicModels.getUntyped(modelName);
            if (model.creator().equals(username)) {
                return publicModels.drop(modelName);
            }
            throw new IllegalStateException(formatWithLocale("Only the creator of model %s can drop it.", modelName));
        } else {
            return getUserCatalog(username).drop(modelName);
        }
    }

    public static Collection<Model<?, ?>> list(String username) {
        var models = new ArrayList<>(getUserCatalog(username).list());
        models.addAll(publicModels.list());
        return models;
    }

    public static @Nullable Model<?, ?> list(String username, String modelName) {
        return getUntyped(username, modelName, false);
    }

    public static Model<?, ?> publish(String username, String modelName) {
        if (GdsEdition.instance().isOnCommunityEdition()) {
            throw new IllegalArgumentException("Publishing a model is only available with the Graph Data Science library Enterprise Edition.");
        }

        Model<?, ?> model = getUntyped(username, modelName);
        // not published => publish it
        if (!model.sharedWith().contains(Model.ALL_USERS)) {
            Model<?, ?> publicModel = model.publish();
            publicModels.set(publicModel);
            drop(username, modelName);
            return publicModel;
        }

        // already published, return it
        return model;
    }

    public static int size() {
        return userCatalogs
            .values()
            .stream()
            .mapToInt(userCatalog -> userCatalog.list().size())
            .sum();
    }

    public static void removeAllLoadedModels() {
        userCatalogs.clear();
        publicModels.removeAllLoadedModels();
    }

    public static void checkStorable(String username, String modelType) {
        getUserCatalog(username).canStoreModel(modelType);
    }

    private static UserCatalog getUserCatalog(String username) {
        return userCatalogs.getOrDefault(username, UserCatalog.EMPTY);
    }

    static class UserCatalog {
        private static final long ALLOWED_MODELS_COUNT = 1;
        private static final UserCatalog EMPTY = new UserCatalog();

        private final Map<String, Model<?, ?>> userModels = new ConcurrentHashMap<>();

        public void set(Model<?, ?> model) {
            canStoreModel(model.algoType());
            if (exists(model.name())) {
                throw new IllegalArgumentException(formatWithLocale(
                    "Model with name `%s` already exists",
                    model.name()
                ));
            }
            userModels.put(model.name(), model);
        }

        public void setUnsafe(Model<?, ?> model) {
            userModels.put(model.name(), model);
        }

        public <D, C extends ModelConfig & BaseConfig> Model<D, C> get(
            String modelName,
            Class<D> dataClass,
            Class<C> configClass
        ) {
            return get(
                getUntyped(modelName),
                dataClass,
                configClass
            );
        }

        public <D, C extends ModelConfig & BaseConfig> Model<D, C> getChecked(
            String modelName,
            Class<D> dataClass,
            Class<C> configClass
        ) {
            return get(
                getUntypedChecked(modelName),
                dataClass,
                configClass
            );
        }

        private <D, C extends ModelConfig & BaseConfig> Model<D, C> get(
            Model<?, ?> model,
            Class<D> dataClass,
            Class<C> configClass
        ) {
            if (model != null) {
                var data = model.data();
                var modelName = model.name();
                if (!dataClass.isInstance(data)) {
                    throw new IllegalArgumentException(formatWithLocale(
                        "The model `%s` has data with different types than expected. " +
                        "Expected data type: `%s`, invoked with model data type: `%s`.",
                        modelName,
                        data.getClass().getName(),
                        dataClass.getName()
                    ));
                }
                var config = model.trainConfig();
                if (!configClass.isInstance(config)) {
                    throw new IllegalArgumentException(formatWithLocale(
                        "The model `%s` has a training config with different types than expected. " +
                        "Expected train config type: `%s`, invoked with model config type: `%s`.",
                        modelName,
                        config.getClass().getName(),
                        configClass.getName()
                    ));
                }
            }

            // We just did the check
            // noinspection unchecked
            return (Model<D, C>) model;
        }

        public boolean exists(String modelName) {
            return userModels.containsKey(modelName);
        }

        public Optional<String> type(String modelName) {
            return Optional.ofNullable(userModels.get(modelName))
                .map(Model::algoType);
        }

        public Model<?, ?> drop(String modelName) {
            var model = getUntypedChecked(modelName);
            return userModels.remove(model.name());
        }

        public Collection<Model<?, ?>> list() {
            return userModels.values();
        }

        public Model<?, ?> list(String modelName) {
            return getUntyped(modelName);
        }

        public void removeAllLoadedModels() {
            userModels.clear();
        }

        public UserCatalog join(UserCatalog other) {
            userModels.putAll(other.userModels);
            return this;
        }

        private boolean reachedModelsLimit(String modelType) {
            return modelsPerType(modelType) == ALLOWED_MODELS_COUNT;
        }

        private long modelsPerType(String modelType) {
            return userModels.values()
                .stream()
                .filter(model -> model.algoType().equals(modelType))
                .count();
        }

        private void canStoreModel(String modelType) {
            if (GdsEdition.instance().isOnCommunityEdition() && reachedModelsLimit(modelType)) {
                throw new IllegalArgumentException("Community users can only store one model in the catalog, see https://neo4j.com/docs/graph-data-science/");
            }
        }

        private Model<?, ?> getUntyped(String modelName) {
            return userModels.get(modelName);
        }

        private Model<?, ?> getUntypedChecked(String modelName) {
            Model<?, ?> model = userModels.get(modelName);
            if (model == null) {
                throw new NoSuchElementException(prettySuggestions(
                    formatWithLocale("Model with name `%s` does not exist.", modelName),
                    modelName,
                    userModels.keySet()
                ));
            }
            return model;
        }
    }
}
