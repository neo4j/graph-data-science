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
package org.neo4j.gds.embeddings.graphsage;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.embeddings.graphsage.algo.ImmutableGraphSageTrainConfig;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatNoException;

@GdlExtension
class GraphSageTrainingConfigValidationTest {

    @GdlGraph
    public static String GRAPH = "(a:Person {weight: 80, age: 42, height: 160})" +
                                 "(b:Instrument {cost: 1337})" +
                                 "(a)-[:KNOWS]->(b)";

    @Inject
    GraphStore graphStore;

    @Test
    void testMissingPropertyForSingleLabel() {
        var singleLabelConfig = ImmutableGraphSageTrainConfig.builder()
            .modelName("singleLabel")
            .addFeatureProperties("doesnotexist")
            .addNodeLabel("Person")
            .build();

        var trainingConfigValidation = new GraphSageTrainingConfigValidation();

        assertThatIllegalArgumentException()
            .isThrownBy(() -> trainingConfigValidation.validateConfigsAfterLoad(graphStore, null, singleLabelConfig))
            .withMessage(
                "The following node properties are not present for each label in the graph: [doesnotexist]." +
                " Properties that exist for each label are [weight, age, height]");
    }

    @Test
    void testMissingPropertyForMultiLabel() {
        var singleLabelConfig = ImmutableGraphSageTrainConfig.builder()
            .modelName("singleLabel")
            .addFeatureProperties("doesnotexist")
            .addNodeLabels("Person", "Instrument")
            .projectedFeatureDimension(4)
            .build();

        var trainingConfigValidation = new GraphSageTrainingConfigValidation();

        assertThatIllegalArgumentException()
            .isThrownBy(() -> trainingConfigValidation.validateConfigsAfterLoad(graphStore, null, singleLabelConfig))
            .withMessage("Each property set in `featureProperties` must exist for at least one label. Missing properties: [doesnotexist]");
    }

    @Test
    void testValidConfiguration() {
        var singleLabelConfig = ImmutableGraphSageTrainConfig.builder()
            .modelName("singleLabel")
            .addFeatureProperties("age", "height", "weight")
            .addNodeLabel("Person")
            .addRelationshipType("KNOWS")
            .build();

        var trainingConfigValidation = new GraphSageTrainingConfigValidation();


        assertThatNoException().isThrownBy(
            () -> trainingConfigValidation.validateConfigsAfterLoad(graphStore, null, singleLabelConfig)
        );

        var multiLabelConfig = ImmutableGraphSageTrainConfig.builder()
            .from(singleLabelConfig)
            .addFeatureProperties("cost")
            .modelName("multiLabel")
            .addNodeLabel("Instrument")
            .addRelationshipType("KNOWS")
            .projectedFeatureDimension(4)
            .build();

        assertThatNoException().isThrownBy(
            () -> trainingConfigValidation.validateConfigsAfterLoad(graphStore, null, multiLabelConfig)
        );
    }

}
