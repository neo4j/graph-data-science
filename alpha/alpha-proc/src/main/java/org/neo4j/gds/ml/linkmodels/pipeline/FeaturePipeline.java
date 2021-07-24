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
package org.neo4j.gds.ml.linkmodels.pipeline;

import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.LinkFeatureStep;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.LinkFeatureStepFactory;
import org.neo4j.graphalgo.BaseProc;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.kernel.database.NamedDatabaseId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.LinkFeatureExtractor.extractFeatures;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public class FeaturePipeline {
    private List<ProcedureStep> procedureSteps;
    private List<LinkFeatureStep> linkFeatureSteps;
    private final String userName;
    private final NamedDatabaseId databaseId;
    private final BaseProc caller;

    public FeaturePipeline(BaseProc caller, NamedDatabaseId databaseId, String userName) {
        this.caller = caller;
        this.userName = userName;
        this.databaseId = databaseId;
        this.linkFeatureSteps = new ArrayList<>();
        this.procedureSteps = new ArrayList<>();
    }

    public void addLinkFeature(String name, Map<String, Object> config) {
        this.linkFeatureSteps.add(LinkFeatureStepFactory.create(name, config));
    }

    public void addProcedureStep(String name, Map<String, Object> config) {
        this.procedureSteps.add(new ProcedureStep(name, config));
    }


    public HugeObjectArray<double[]> computeFeatures(String graphName, Collection<NodeLabel> nodeLabels, Collection<RelationshipType> relationshipTypes) {
        executeProcedureSteps(graphName, nodeLabels, relationshipTypes);
        var graph = GraphStoreCatalog.get(userName, databaseId, graphName)
            .graphStore()
            .getGraph(nodeLabels, relationshipTypes, Optional.empty());
        validate(graph);

        return extractFeatures(graph, linkFeatureSteps);
    }

    private void executeProcedureSteps(
        String graphName,
        Collection<NodeLabel> nodeLabels,
        Collection<RelationshipType> relationshipTypes
    ) {
        for (ProcedureStep step : procedureSteps) {
            step.execute(caller, graphName, nodeLabels, relationshipTypes);
        }
    }

    private void validate(Graph graph) {
        Set<String> graphProperties = graph.availableNodeProperties();

        var invalidProperties = linkFeatureSteps
            .stream()
            .flatMap(step -> step.inputNodeProperties().stream())
            .filter(property -> !graphProperties.contains(property))
            .collect(Collectors.toList());

        if (!invalidProperties.isEmpty()) {
            throw new IllegalArgumentException(formatWithLocale("Node properties %s defined in the LinkFeatureSteps do not exist in the graph or part of the pipeline", invalidProperties));
        }
    }
}
