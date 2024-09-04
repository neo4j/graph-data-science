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
package org.neo4j.gds.applications.algorithms.miscellaneous;

import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplateConvenience;
import org.neo4j.gds.applications.algorithms.machinery.MutateNodeProperty;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.applications.algorithms.metadata.RelationshipsWritten;
import org.neo4j.gds.core.loading.SingleTypeRelationships;
import org.neo4j.gds.indexInverse.InverseRelationshipsConfig;
import org.neo4j.gds.scaleproperties.ScalePropertiesMutateConfig;
import org.neo4j.gds.scaleproperties.ScalePropertiesResult;
import org.neo4j.gds.undirected.ToUndirectedConfig;
import org.neo4j.gds.walking.CollapsePathConfig;

import java.util.Map;

import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.CollapsePath;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.IndexInverse;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.ScaleProperties;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.ToUndirected;

public class MiscellaneousApplicationsMutateModeBusinessFacade {
    private final MiscellaneousApplicationsEstimationModeBusinessFacade estimation;
    private final MiscellaneousAlgorithms algorithms;
    private final AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience;
    private final MutateNodeProperty mutateNodeProperty;

    MiscellaneousApplicationsMutateModeBusinessFacade(
        MiscellaneousApplicationsEstimationModeBusinessFacade estimation,
        MiscellaneousAlgorithms algorithms,
        AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience,
        MutateNodeProperty mutateNodeProperty
    ) {
        this.estimation = estimation;
        this.algorithms = algorithms;
        this.algorithmProcessingTemplateConvenience = algorithmProcessingTemplateConvenience;
        this.mutateNodeProperty = mutateNodeProperty;
    }

    public <RESULT> RESULT collapsePath(
        GraphName graphName,
        CollapsePathConfig configuration,
        ResultBuilder<CollapsePathConfig, SingleTypeRelationships, RESULT, Void> resultBuilder
    ) {
        var mutateStep = new CollapsePathMutateStep();

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateMode(
            graphName,
            configuration,
            CollapsePath,
            estimation::collapsePath,
            (__, graphStore) -> algorithms.collapsePath(graphStore, configuration),
            mutateStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT indexInverse(
        GraphName graphName,
        InverseRelationshipsConfig configuration,
        ResultBuilder<InverseRelationshipsConfig, Map<RelationshipType, SingleTypeRelationships>, RESULT, Void> resultBuilder
    ) {
        var mutateStep = new IndexInverseMutateStep();

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateMode(
            graphName,
            configuration,
            IndexInverse,
            () -> estimation.indexInverse(configuration),
            (graph, graphStore) -> algorithms.indexInverse(graph, graphStore, configuration),
            mutateStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT scaleProperties(
        GraphName graphName,
        ScalePropertiesMutateConfig configuration,
        ResultBuilder<ScalePropertiesMutateConfig, ScalePropertiesResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new ScalePropertiesMutateStep(mutateNodeProperty, configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateMode(
            graphName,
            configuration,
            ScaleProperties,
            () -> estimation.scaleProperties(configuration),
            (graph, __) -> algorithms.scaleProperties(graph, configuration),
            mutateStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT toUndirected(
        GraphName graphName,
        ToUndirectedConfig configuration,
        ResultBuilder<ToUndirectedConfig, SingleTypeRelationships, RESULT, RelationshipsWritten> resultBuilder
    ) {
        var mutateStep = new ToUndirectedMutateStep();

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateMode(
            graphName,
            configuration,
            ToUndirected,
            () -> estimation.toUndirected(configuration),
            (graph, graphStore) -> algorithms.toUndirected(graphStore, configuration),
            mutateStep,
            resultBuilder
        );
    }
}
