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

import org.neo4j.gds.BaseProc;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.pipeline.proc.ProcedureReflection;
import org.neo4j.gds.ml.splitting.SplitRelationshipsBaseConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import static org.neo4j.gds.config.RelationshipWeightConfig.RELATIONSHIP_WEIGHT_PROPERTY;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class RelationshipSplitter {

    private static final String SPLIT_ERROR_TEMPLATE = "%s graph contains no relationships. Consider increasing the `%s` or provide a larger graph";

    private final String graphName;
    private final LinkPredictionSplitConfig splitConfig;
    private final BaseProc caller;
    private final ProgressTracker progressTracker;

    RelationshipSplitter(
        String graphName,
        LinkPredictionSplitConfig splitConfig,
        BaseProc caller,
        ProgressTracker progressTracker
    ) {
        this.graphName = graphName;
        this.splitConfig = splitConfig;
        this.caller = caller;
        this.progressTracker = progressTracker;
    }

    public void splitRelationships(
        GraphStore graphStore,
        List<String> relationshipTypes,
        List<String> nodeLabels,
        Optional<Long> randomSeed,
        Optional<String> relationshipWeightProperty
    ) {
        progressTracker.beginSubTask();

        splitConfig.validateAgainstGraphStore(graphStore);

        var testComplementRelationshipType = splitConfig.testComplementRelationshipType();

        // Relationship sets: test, train, feature-input, test-complement. The nodes are always the same.
        // 1. Split base graph into test, test-complement
        //      Test also includes newly generated negative links, that were not in the base graph (and positive links).
        relationshipSplit(splitConfig.testSplit(), nodeLabels, relationshipTypes, randomSeed, relationshipWeightProperty);
        validateTestSplit(graphStore);


        // 2. Split test-complement into (labeled) train and feature-input.
        //      Train relationships also include newly generated negative links, that were not in the base graph (and positive links).
        relationshipSplit(splitConfig.trainSplit(), nodeLabels, List.of(testComplementRelationshipType), randomSeed, relationshipWeightProperty);
        validateTrainSplit(graphStore);

        graphStore.deleteRelationships(RelationshipType.of(testComplementRelationshipType));

        progressTracker.endSubTask();
    }

    private void validateTestSplit(GraphStore graphStore) {
        String testRelationshipType = splitConfig.testRelationshipType();
        if (graphStore.getGraph(RelationshipType.of(testRelationshipType)).relationshipCount() <= 0) {
            throw new IllegalStateException(formatWithLocale(
                SPLIT_ERROR_TEMPLATE,
                "Test",
                LinkPredictionSplitConfig.TEST_FRACTION_KEY
            ));
        }
    }

    private void validateTrainSplit(GraphStore graphStore) {
        if (graphStore.getGraph(RelationshipType.of(splitConfig.trainRelationshipType())).relationshipCount() <= 0) {
            throw new IllegalStateException(formatWithLocale(
                SPLIT_ERROR_TEMPLATE,
                "Train",
                LinkPredictionSplitConfig.TRAIN_FRACTION_KEY
            ));
        }

        if (graphStore
                .getGraph(RelationshipType.of(splitConfig.featureInputRelationshipType()))
                .relationshipCount() <= 0)
            throw new IllegalStateException(formatWithLocale(
                "Feature graph contains no relationships. Consider decreasing %s or %s or provide a larger graph.",
                LinkPredictionSplitConfig.TEST_FRACTION_KEY,
                LinkPredictionSplitConfig.TRAIN_FRACTION_KEY
            ));
    }

    private void relationshipSplit(
        SplitRelationshipsBaseConfig splitConfig,
        List<String> nodeLabels,
        List<String> relationshipTypes,
        Optional<Long> randomSeed,
        Optional<String> relationshipWeightProperty
    ) {
        var splitRelationshipProcConfig = new HashMap<>(splitConfig.toSplitMap()) {{
            put("nodeLabels", nodeLabels);
            put("relationshipTypes", relationshipTypes);
            relationshipWeightProperty.ifPresent(s -> put(RELATIONSHIP_WEIGHT_PROPERTY, s));
            randomSeed.ifPresent(seed -> put("randomSeed", seed));
        }};

        var procReflection = ProcedureReflection.INSTANCE;
        procReflection.invokeProc(caller, graphName, procReflection.findProcedureMethod("splitRelationships"), splitRelationshipProcConfig);
    }
}
