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

import org.neo4j.gds.ElementProjection;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.pipeline.linkPipeline.train.LinkPredictionTrainConfig;
import org.neo4j.gds.utils.StringJoining;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.gds.config.ElementIdentityResolver.resolveAndValidateTypes;
import static org.neo4j.gds.config.ElementIdentityResolver.resolveTypes;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class LPGraphStoreFilterFactory {


    private LPGraphStoreFilterFactory() {}

    private static Collection<NodeLabel> internalNodeLabels(GraphStore graphStore, String nodeLabel) {
        return (nodeLabel.equals(ElementProjection.PROJECT_ALL)) ? graphStore.nodeLabels() : NodeLabel.listOf(nodeLabel);
    }

    private static Collection<NodeLabel> nodePropertyStepLabels(
        GraphStore graphStore,
        String sourceNodeLabel,
        String targetNodeLabel,
        List<String> contextNodeLabels
    ) {
        return (contextNodeLabels.contains(ElementProjection.PROJECT_ALL)
                || sourceNodeLabel.equals(ElementProjection.PROJECT_ALL) || targetNodeLabel.equals(ElementProjection.PROJECT_ALL))
            ? graphStore.nodeLabels()
            : Stream.concat(contextNodeLabels.stream(), Stream.of(sourceNodeLabel, targetNodeLabel))
                .map(NodeLabel::of)
                .collect(Collectors.toSet());
    }

    public static LPGraphStoreFilter generate(
        LinkPredictionTrainConfig trainConfig,
        LinkPredictionPredictPipelineBaseConfig predictConfig,
        GraphStore graphStore,
        ProgressTracker progressTracker
    ) {
        String sourceNodeLabel = predictConfig.sourceNodeLabel().orElse(trainConfig.sourceNodeLabel());
        String targetNodeLabel = predictConfig.targetNodeLabel().orElse(trainConfig.targetNodeLabel());
        List<String> contextNodeLabels;
        if (!predictConfig.contextNodeLabels().isEmpty()) {
            contextNodeLabels = predictConfig.contextNodeLabels();
        } else {
            contextNodeLabels = trainConfig.contextNodeLabels();
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

        Set<RelationshipType> nodePropertyStepRelTypes = Stream.of(contextRelTypes, predictRelTypes)
            .flatMap(Collection::stream)
            .collect(Collectors.toSet());

        LPGraphStoreFilter filter = ImmutableLPGraphStoreFilter.builder()
            .sourceNodeLabels(internalNodeLabels(graphStore, sourceNodeLabel))
            .targetNodeLabels(internalNodeLabels(graphStore, targetNodeLabel))
            .nodePropertyStepsLabels(nodePropertyStepLabels(graphStore, sourceNodeLabel, targetNodeLabel, contextNodeLabels))
            .predictRelationshipTypes(predictRelTypes)
            .nodePropertyStepRelationshipTypes(nodePropertyStepRelTypes)
            .build();

        progressTracker.logInfo(formatWithLocale("The graph filters used for filtering in prediction is %s", filter));

        validateGraphFilter(graphStore, filter);

        return filter;
    }

    private static void validateGraphFilter(GraphStore graphStore, LPGraphStoreFilter filter) {
        var nodePropertyStepsLabels = filter.nodePropertyStepsLabels();
        var invalidLabels = nodePropertyStepsLabels
            .stream()
            .filter((label -> !graphStore.nodeLabels().contains(label)))
            .map(NodeLabel::name)
            .collect(Collectors.toList());

        if (!invalidLabels.isEmpty()) {
            throw new IllegalArgumentException(formatWithLocale(
                "Based on the predict and the model's training configuration, expected node labels %s, but could not find %s. Available labels are %s.",
                StringJoining.join(nodePropertyStepsLabels.stream().map(NodeLabel::name)),
                StringJoining.join(invalidLabels),
                StringJoining.join(graphStore.nodeLabels().stream().map(NodeLabel::name))
            ));
        }

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
}
