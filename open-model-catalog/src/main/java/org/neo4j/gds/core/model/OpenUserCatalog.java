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

import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static org.neo4j.gds.core.StringSimilarity.prettySuggestions;

class OpenUserCatalog implements UserCatalog {
    private static final long ALLOWED_MODELS_COUNT = 3;

    private final Map<String, Model<?, ?, ?>> userModels = new ConcurrentHashMap<>();

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
    public void verifyModelCanBeStored(String modelName, ModelType modelType) {
        verifyModelNameIsUnique(modelName);
        verifyModelsLimit(modelType);
    }

    @Override
    public long size() {
        return userModels.size();
    }

    private void verifyModelNameIsUnique(String model) {
        if (exists(model)) {
            throw new IllegalArgumentException(formatWithLocale(
                "Model with name `%s` already exists.",
                model
            ));
        }
    }

    private void verifyModelsLimit(ModelType modelType) {
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

    private boolean canStoreModel(ModelType modelType) {
        return modelsPerType(modelType) < ALLOWED_MODELS_COUNT;
    }

    private long modelsPerType(ModelType modelType) {
        return userModels.values()
            .stream()
            .filter(model -> model.algoType().equals(modelType))
            .count();
    }

    private static String formatWithLocale(String template, Object... inputs) {
        return String.format(Locale.ENGLISH, template, inputs);
    }
}
