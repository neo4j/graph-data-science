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
package org.neo4j.gds.ml.pipeline.node;

import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.ElementTypeValidator;
import org.neo4j.gds.ml.pipeline.ImmutablePipelineGraphFilter;
import org.neo4j.gds.ml.pipeline.PipelineGraphFilter;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodePropertyPipelineBaseTrainConfig;

import java.util.Collection;

import static org.neo4j.gds.config.ElementTypeValidator.resolveAndValidateTypes;

public class NodePipelinePredictGraphFilterutil {

    public static PipelineGraphFilter generatePredictGraphFilter (
        GraphStore graphStore,
        NodePropertyPredictPipelineBaseConfig configuration,
        NodePropertyPipelineBaseTrainConfig trainConfig
    ) {
        var targetNodeLabels = configuration.targetNodeLabels().isEmpty()
            ? ElementTypeValidator.resolveAndValidate(graphStore, trainConfig.targetNodeLabels(), "`targetNodeLabels` from the model's train config")
            : ElementTypeValidator.resolve(graphStore, configuration.targetNodeLabels());

        Collection<RelationshipType> predictRelTypes;
        if (!configuration.relationshipTypes().isEmpty()) {
            predictRelTypes = resolveAndValidateTypes(graphStore, configuration.relationshipTypes(), "`relationshipTypes` from the model's predict config");
        } else {
            predictRelTypes = resolveAndValidateTypes(
                graphStore,
                trainConfig.relationshipTypes(),
                "`relationshipTypes` from the model's train config"
            );
        }
        var predictGraphFilter = ImmutablePipelineGraphFilter.builder()
            .nodeLabels(targetNodeLabels)
            .relationshipTypes(predictRelTypes)
            .build();
        return predictGraphFilter;
    }
}
