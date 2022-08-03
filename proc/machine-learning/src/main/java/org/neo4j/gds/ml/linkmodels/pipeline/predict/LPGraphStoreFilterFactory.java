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
package org.neo4j.gds.ml.linkmodels.pipeline.predict;

import com.carrotsearch.hppc.predicates.LongPredicate;
import org.neo4j.gds.ElementProjection;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.ElementTypeValidator;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.pipeline.linkPipeline.train.LinkPredictionTrainConfig;
import org.neo4j.gds.utils.StringJoining;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.gds.config.ElementTypeValidator.resolveAndValidateTypes;
import static org.neo4j.gds.config.ElementTypeValidator.resolveTypes;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class LPGraphStoreFilterFactory {


    private LPGraphStoreFilterFactory() {}

    public static LPGraphStoreFilter generate(
        LinkPredictionTrainConfig trainConfig,
        LinkPredictionPredictPipelineBaseConfig predictConfig,
        GraphStore graphStore,
        ProgressTracker progressTracker
    ) {
        var sourceNodeLabels = predictConfig
            .sourceNodeLabel()
            .map(label -> ElementTypeValidator.resolve(graphStore, List.of(label)))
            .orElse(ElementTypeValidator.resolveAndValidate(graphStore, List.of(trainConfig.sourceNodeLabel()), "`sourceNodeLabel` from the model's train config"));

        var targetNodeLabels = predictConfig
            .targetNodeLabel()
            .map(label -> ElementTypeValidator.resolve(graphStore, List.of(label)))
            .orElse(ElementTypeValidator.resolveAndValidate(graphStore, List.of(trainConfig.targetNodeLabel()), "`targetNodeLabel` from the model's train config"));

        Collection<NodeLabel> contextNodeLabels;
        if (!predictConfig.contextNodeLabels().isEmpty()) {
            contextNodeLabels = ElementTypeValidator.resolve(graphStore, predictConfig.contextNodeLabels());
        } else {
            contextNodeLabels = ElementTypeValidator.resolveAndValidate(graphStore, trainConfig.contextNodeLabels(), "`contextNodeLabels` from the model's train config");
        }

        Collection<RelationshipType> contextRelTypes;
        if (!predictConfig.contextRelationshipTypes().isEmpty()) {
            contextRelTypes = resolveTypes(graphStore, predictConfig.contextRelationshipTypes());
        } else {
            contextRelTypes = resolveAndValidateTypes(
                graphStore,
                trainConfig.contextRelationshipTypes(),
                "`contextRelationshipTypes` from the model's train config"
            );
        }

        Collection<RelationshipType> predictRelTypes;
        if (!predictConfig.relationshipTypes().isEmpty()) {
            predictRelTypes = resolveTypes(graphStore, predictConfig.relationshipTypes());
        } else {
            predictRelTypes = resolveAndValidateTypes(
                graphStore,
                List.of(trainConfig.targetRelationshipType()),
                "`targetRelationshipType` from the model's train config"
            );
        }

        var nodePropertyStepRelTypes = Stream.of(contextRelTypes, predictRelTypes)
            .flatMap(Collection::stream)
            .collect(Collectors.toSet());

        var nodePropertyStepsLabels = Stream
            .of(contextNodeLabels, targetNodeLabels, sourceNodeLabels)
            .flatMap(Collection::stream)
            .collect(Collectors.toSet());

        LPGraphStoreFilter filter = ImmutableLPGraphStoreFilter.builder()
            .sourceNodeLabels(sourceNodeLabels)
            .targetNodeLabels(targetNodeLabels)
            .nodePropertyStepsLabels(nodePropertyStepsLabels)
            .predictRelationshipTypes(predictRelTypes)
            .nodePropertyStepRelationshipTypes(nodePropertyStepRelTypes)
            .build();

        progressTracker.logInfo(formatWithLocale("The graph filters used for filtering in prediction is %s", filter));

        validateGraphFilter(graphStore, filter);

        return filter;
    }

    private static void validateGraphFilter(GraphStore graphStore, LPGraphStoreFilter filter) {
        var directedPredictRels = filter
            .predictRelationshipTypes()
            .stream()
            .filter(type -> !graphStore.isUndirected(type))
            .map(RelationshipType::name)
            .collect(Collectors.toList());

        if (!directedPredictRels.isEmpty()) {
            throw new IllegalArgumentException(formatWithLocale(
                "Procedure requires all relationships of %s to be UNDIRECTED, but found %s to be directed.",
                StringJoining.join(filter.predictRelationshipTypes().stream().map(RelationshipType::name)),
                StringJoining.join(directedPredictRels)
            ));
        }
    }

    public static LongPredicate generateNodeLabelFilter(Graph predictGraph, IdMap idMap) {
        // IdMap can only contain nodes that are in the predictGraph.
        if (predictGraph.nodeCount() == idMap.nodeCount()) {
            return id -> true;
        } else {
            return id -> idMap.contains(predictGraph.toOriginalNodeId(id));
        }
    }
}
