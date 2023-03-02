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

import org.neo4j.gds.core.model.Model.CustomInfo;
import org.neo4j.gds.model.ModelConfig;

import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static org.neo4j.gds.core.StringSimilarity.prettySuggestions;

class OpenUserCatalog implements UserCatalog {
    private static final long ALLOWED_MODELS_COUNT = 3;

    private final Map<String, Model<?, ?, ?>> userModels = new ConcurrentHashMap<>();

    @Override
    public <D, C extends ModelConfig, I extends CustomInfo> Model<D, C, I> get(
        String modelName,
        Class<D> dataClass,
        Class<C> configClass,
        Class<I> infoClass
    ) {
        return get(
            getUntyped(modelName),
            dataClass,
            configClass,
            infoClass
        );
    }

    @Override
    public Model<?, ?, ?> getUntyped(String modelName) {
        return userModels.get(modelName);
    }

    @Override
    public void set(Model<?, ?, ?> model) {
        verifyModelCanBeStored(model.name(), model.algoType());
        userModels.put(model.name(), model);
    }

    @Override
    public Collection<Model<?, ?, ?>> list() {
        return userModels.values();
    }

    @Override
    public Model<?, ?, ?> list(String modelName) {
        return getUntyped(modelName);
    }

    @Override
    public Stream<Model<?, ?, ?>> streamModels() {
        return userModels.values().stream();
    }

    @Override
    public Set<String> availableModelNames() {
        return userModels.keySet();
    }

    @Override
    public boolean exists(String modelName) {
        return userModels.containsKey(modelName);
    }

    @Override
    public Optional<String> type(String modelName) {
        return Optional.ofNullable(userModels.get(modelName)).map(Model::algoType);
    }

    @Override
    public Model<?, ?, ?> drop(String modelName, boolean failOnMissing) {
        var storedModel = userModels.remove(modelName);

        if (failOnMissing && storedModel == null) {
            throw new NoSuchElementException(prettySuggestions(
                formatWithLocale("Model with name `%s` does not exist.", modelName),
                modelName,
                userModels.keySet()
            ));
        } else {
            return storedModel;
        }
    }

    @Override
    public void removeAllLoadedModels() {
        userModels.clear();
    }

    @Override
    public void verifyModelCanBeStored(String modelName, String modelType) {
        verifyModelNameIsUnique(modelName);
        verifyModelsLimit(modelType);
    }

    @Override
    public long size() {
        return userModels.size();
    }

    private <D, C extends ModelConfig, I extends CustomInfo> Model<D, C, I> get(
        Model<?, ?, ?> model,
        Class<D> dataClass,
        Class<C> configClass,
        Class<I> infoClass
    ) {
        if (model != null) {
            var modelName = model.name();
            if (!dataClass.isInstance(model.data())) {
                throw new IllegalArgumentException(formatWithLocale(
                    "The model `%s` has data with different types than expected. " +
                    "Expected data type: `%s`, invoked with model data type: `%s`.",
                    modelName,
                    model.data().getClass().getName(),
                    dataClass.getName()
                ));
            }
            if (!configClass.isInstance(model.trainConfig())) {
                throw new IllegalArgumentException(formatWithLocale(
                    "The model `%s` has a training config with different types than expected. " +
                    "Expected train config type: `%s`, invoked with model config type: `%s`.",
                    modelName,
                    model.trainConfig().getClass().getName(),
                    configClass.getName()
                ));
            }


            if (!infoClass.isInstance(model.customInfo())) {
                throw new IllegalArgumentException(formatWithLocale(
                    "The model `%s` has a customInfo with different types than expected. " +
                    "Expected customInfo type: `%s`, invoked with model info type: `%s`.",
                    modelName,
                    model.customInfo().getClass().getName(),
                    infoClass.getName()
                ));
            }
        }

        // We just did the check
        // noinspection unchecked
        return (Model<D, C, I>) model;
    }

    private void verifyModelNameIsUnique(String model) {
        if (exists(model)) {
            throw new IllegalArgumentException(formatWithLocale(
                "Model with name `%s` already exists.",
                model
            ));
        }
    }

    private void verifyModelsLimit(String modelType) {
        if (!canStoreModel(modelType)) {
            throw new IllegalStateException(
                formatWithLocale(
                    "Storing more than `%d` models in the catalog is not available in openGDS. " +
                    "Please consider licensing the Graph Data Science library. " +
                    "See documentation at https://neo4j.com/docs/graph-data-science/",
                    ALLOWED_MODELS_COUNT
                )
            );
        }
    }

    private boolean canStoreModel(String modelType) {
        return modelsPerType(modelType) < ALLOWED_MODELS_COUNT;
    }

    private long modelsPerType(String modelType) {
        return userModels.values()
            .stream()
            .filter(model -> model.algoType().equals(modelType))
            .count();
    }

    private static String formatWithLocale(String template, Object... inputs) {
        return String.format(Locale.ENGLISH, template, inputs);
    }
}
