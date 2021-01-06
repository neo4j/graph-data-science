/*
 * Copyright (c) 2017-2021 "Neo4j,"
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

import org.neo4j.graphalgo.config.BaseConfig;
import org.neo4j.graphalgo.config.ModelConfig;
import org.neo4j.graphalgo.core.GdsEdition;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public final class ModelCatalog {

    private ModelCatalog() {}

    private static final Map<String, UserCatalog> userCatalogs = new ConcurrentHashMap<>();

    public static void set(Model<?, ?> model) {
        userCatalogs.compute(model.username(), (user, userCatalog) -> {
            if (userCatalog == null) {
                userCatalog = new UserCatalog();
            }
            userCatalog.set(model);
            return userCatalog;
        });
    }

    public static <D, C extends ModelConfig & BaseConfig> Model<D, C> get(
        String username, String modelName, Class<D> dataClass, Class<C> configClass
    ) {
        return getUserCatalog(username).get(modelName, dataClass, configClass);
    }

    public static boolean exists(String username, String modelName) {
        return getUserCatalog(username).exists(modelName);
    }

    public static Optional<String> type(String username, String modelName) {
        return getUserCatalog(username).type(modelName);
    }

    public static Model<?, ?> drop(String username, String modelName) {
        return getUserCatalog(username).drop(modelName);
    }

    public static Collection<Model<?, ?>> list(String username) {
        return getUserCatalog(username).list();
    }

    public static Model<?, ?> list(String username, String modelName) {
        return getUserCatalog(username).list(modelName);
    }

    public static void removeAllLoadedModels() {
        userCatalogs.clear();
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

        public <D, C extends ModelConfig & BaseConfig> Model<D, C> get(
            String modelName,
            Class<D> dataClass,
            Class<C> configClass
        ) {
            Model<?, ?> model = get(modelName);

            var data = model.data();
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
            Model<?, ?> model = userModels.remove(modelName);
            if (model == null) {
                throw new IllegalArgumentException(formatWithLocale(
                    "Model with name `%s` does not exist and can't be removed.",
                    modelName
                ));
            }
            return model;
        }

        public Collection<Model<?, ?>> list() {
            return userModels.values();
        }

        public Model<?, ?> list(String modelName) {
            return get(modelName);
        }

        public void removeAllLoadedModels() {
            userModels.clear();
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
                throw new IllegalArgumentException("Community users can only store one model in the catalog");
            }
        }

        private Model<?, ?> get(String modelName) {
            Model<?, ?> model = userModels.get(modelName);
            if (model == null) {
                throw new IllegalArgumentException(formatWithLocale(
                    "No model with model name `%s` was found.",
                    modelName
                ));
            }
            return model;
        }
    }
}
