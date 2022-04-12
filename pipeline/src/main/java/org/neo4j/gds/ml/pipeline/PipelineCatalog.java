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
package org.neo4j.gds.ml.pipeline;

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionTrainingPipeline;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.NodeClassificationTrainingPipeline;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class PipelineCatalog {

    private static final ConcurrentHashMap<String, PipelineUserCatalog> userCatalogs = new ConcurrentHashMap<>();

    private static final Map<Class<?>, String> classToType = Map.of(
        NodeClassificationTrainingPipeline.class, NodeClassificationTrainingPipeline.PIPELINE_TYPE,
        LinkPredictionTrainingPipeline.class, LinkPredictionTrainingPipeline.PIPELINE_TYPE
    );

    private PipelineCatalog() { }

    public static void set(String user, String pipelineName, TrainingPipeline<?> pipeline) {
        userCatalogs.computeIfAbsent(user, ignore -> new PipelineUserCatalog()).set(pipelineName, pipeline);
    }

    public static boolean exists(String user, String pipelineName) {
        return userCatalogs.getOrDefault(user, PipelineUserCatalog.EMPTY).exists(pipelineName);
    }

    public static TrainingPipeline<?> get(String user, String pipelineName) {
        return userCatalogs.getOrDefault(user, PipelineUserCatalog.EMPTY)
            .get(pipelineName)
            .orElseThrow(() -> createPipelineNotFoundException(user, pipelineName));
    }

    public static <PIPELINE extends TrainingPipeline<?>> PIPELINE getTyped(String user, String pipelineName, Class<PIPELINE> expectedClass) {
        TrainingPipeline<?> pipeline = get(user, pipelineName);

        if (!expectedClass.isInstance(pipeline)) {
            throw new IllegalArgumentException(formatWithLocale(
                "The pipeline `%s` is of type `%s`, but expected type `%s`.",
                pipelineName,
                pipeline.type(),
                classToType.getOrDefault(expectedClass, expectedClass.getSimpleName())
            ));
        }

        return (PIPELINE) pipeline;
    }

    public static TrainingPipeline<?> drop(String user, String pipelineName) {
        return userCatalogs.getOrDefault(user, PipelineUserCatalog.EMPTY)
            .drop(pipelineName)
            .orElseThrow(() -> createPipelineNotFoundException(user, pipelineName));
    }

    public static void removeAll() {
        userCatalogs.clear();
    }

    public static Stream<PipelineCatalogEntry> getAllPipelines(String user) {
        return userCatalogs.getOrDefault(user, PipelineUserCatalog.EMPTY).stream();
    }

    @NotNull
    private static NoSuchElementException createPipelineNotFoundException(String user, String pipelineName) {
        return new NoSuchElementException(formatWithLocale(
            "Pipeline with name `%s` does not exist for user `%s`.",
            pipelineName,
            user
        ));
    }

    @ValueClass
    public interface PipelineCatalogEntry {
        String pipelineName();

        TrainingPipeline<?> pipeline();
    }

    static class PipelineUserCatalog {

        private final Map<String, TrainingPipeline<?>> pipelineByName;

        private static final PipelineUserCatalog EMPTY = new PipelineUserCatalog();

        PipelineUserCatalog() {
            this.pipelineByName = new ConcurrentHashMap<>();
        }

        public void set(String pipelineName, TrainingPipeline<?> pipeline) {
            if (pipelineByName.containsKey(pipelineName)) {
                throw new IllegalStateException(formatWithLocale(
                    "Pipeline named `%s` already exists.",
                    pipelineName
                ));
            }

            pipelineByName.put(pipelineName, pipeline);
        }

        public boolean exists(String pipelineName) {
            return pipelineByName.containsKey(pipelineName);
        }

        public Optional<TrainingPipeline<?>> get(String pipelineName) {
            return Optional.ofNullable(pipelineByName.get(pipelineName));
        }

        public Optional<TrainingPipeline<?>> drop(String pipelineName) {
            return Optional.ofNullable(pipelineByName.remove(pipelineName));
        }

        Stream<PipelineCatalogEntry> stream() {
            return pipelineByName.entrySet().stream().map(mapEntry -> ImmutablePipelineCatalogEntry.of(mapEntry.getKey(), mapEntry.getValue()));
        }
    }
}
