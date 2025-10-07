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
package org.neo4j.gds.pregel.proc;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.properties.nodes.NodePropertyRecord;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodePropertyValuesAdapter;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.applications.algorithms.miscellaneous.MiscellaneousAlgorithms;
import org.neo4j.gds.beta.pregel.NodeValue;
import org.neo4j.gds.beta.pregel.PregelConfig;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.beta.pregel.PregelSchema;
import org.neo4j.gds.core.PlainSimpleRequestCorrelationId;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.logging.LoggerForProgressTrackingAdapter;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.EmptyUserLogRegistryFactory;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.validation.AfterLoadValidation;
import org.neo4j.gds.executor.validation.ValidationConfiguration;
import org.neo4j.gds.indexInverse.InverseRelationshipsParamsTransformer;
import org.neo4j.gds.indexInverse.InverseRelationshipsTask;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.utils.StringJoining;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class PregelCompanion {

    public static <CONFIG extends PregelConfig> ValidationConfiguration<CONFIG> ensureIndexValidation(
        Log log, TaskRegistryFactory taskRegistryFactory
    ) {
        return new ValidationConfiguration<>() {
            @Override
            public List<AfterLoadValidation<CONFIG>> afterLoadValidations() {
                return List.of(
                    (graphStore, graphProjectConfig, config) -> ensureDirectedRelationships(
                        graphStore, config.internalRelationshipTypes(graphStore)
                    ),
                    (graphStore, graphProjectConfig, config) -> ensureInverseIndexesExist(graphStore,
                        config.internalRelationshipTypes(graphStore),
                        config.concurrency(),
                        log,
                        taskRegistryFactory,
                        config.logProgress()
                    )
                );
            }
        };
    }

    static void ensureInverseIndexesExist(
        GraphStore graphStore,
        Collection<RelationshipType> relationshipTypes,
        Concurrency concurrency,
        Log log,
        TaskRegistryFactory taskRegistryFactory,
        boolean logProgress
    ) {
        var relationshipTypesWithoutIndex = relationshipTypes
            .stream()
            .filter(relType -> !graphStore.inverseIndexedRelationshipTypes().contains(relType))
            .map(RelationshipType::name)
            .collect(Collectors.toList());

        if (relationshipTypesWithoutIndex.isEmpty()) {
            return;
        }

        var  params = InverseRelationshipsParamsTransformer.toParameters(
            graphStore,
            concurrency,
            relationshipTypesWithoutIndex
        );

        var progressTrackerCreator = new ProgressTrackerCreator(
            new LoggerForProgressTrackingAdapter(log),
            RequestScopedDependencies.builder()
                .correlationId(new PlainSimpleRequestCorrelationId())
                .taskRegistryFactory(taskRegistryFactory)
                .userLogRegistryFactory(EmptyUserLogRegistryFactory.INSTANCE)
                .build()
        );

        var task = InverseRelationshipsTask.progressTask(graphStore.nodeCount(), params);

        var progressTracker = progressTrackerCreator.createProgressTracker(task,
            new JobId(),
            params.concurrency(),
            logProgress
        );
        var miscAlgs = new MiscellaneousAlgorithms(
            TerminationFlag.RUNNING_TRUE
        );

       miscAlgs.indexInverse(graphStore,params,progressTracker)
            .forEach((relationshipType, inverseIndex) -> graphStore.addInverseIndex(
                relationshipType,
                inverseIndex.topology(),
                inverseIndex.properties()
            ));
    }

    static void ensureDirectedRelationships(GraphStore graphStore, Collection<RelationshipType> relationshipTypes) {
        var relationshipSchema = graphStore.schema().relationshipSchema();
        var undirectedTypes = relationshipTypes
            .stream()
            .filter(relationshipSchema::isUndirected)
            .map(RelationshipType::name)
            .collect(Collectors.toList());

        if (!undirectedTypes.isEmpty()) {
            throw new IllegalArgumentException(String.format(
                Locale.US,
                "This algorithm requires a directed graph, but the following configured relationship types are undirected: %s.",
                StringJoining.join(undirectedTypes)
            ));
        }
    }

    static <ALGO extends Algorithm<PregelResult>, CONFIG extends PregelConfig> List<NodePropertyRecord> nodeProperties(
        ComputationResult<ALGO, PregelResult, CONFIG> computationResult, String propertyPrefix
    ) {
        if (computationResult.result().isEmpty()) {
            return Collections.emptyList();
        }

        var result = computationResult.result().get();
        var compositeNodeValue = result.nodeValues();
        var schema = compositeNodeValue.schema();
        // TODO change this to generic prefix setting

        return schema
            .elements()
            .stream()
            .filter(element -> element.visibility() == PregelSchema.Visibility.PUBLIC)
            .map(element -> {
                var propertyKey = element.propertyKey();
                var nodePropertyValues  = adaptNodeProperty(
                    propertyKey,
                    element.propertyType(),
                    compositeNodeValue
                );
                return NodePropertyRecord.of(formatWithLocale("%s%s", propertyPrefix, propertyKey), nodePropertyValues);
            }).collect(Collectors.toList());
    }


   private static NodePropertyValues adaptNodeProperty(
       String propertyKey,
       ValueType propertyType,
       NodeValue compositeNodeValue
   ){
        return switch (propertyType) {
            case LONG -> NodePropertyValuesAdapter.adapt(compositeNodeValue.longProperties(propertyKey));
            case DOUBLE -> NodePropertyValuesAdapter.adapt(compositeNodeValue.doubleProperties(propertyKey));
            case LONG_ARRAY -> NodePropertyValuesAdapter.adapt(compositeNodeValue.longArrayProperties(propertyKey));
            case DOUBLE_ARRAY -> NodePropertyValuesAdapter.adapt(compositeNodeValue.doubleArrayProperties(propertyKey));
            default -> throw new IllegalArgumentException("Unsupported property type: " + propertyType);
        };
    }
    private PregelCompanion() {}

}
