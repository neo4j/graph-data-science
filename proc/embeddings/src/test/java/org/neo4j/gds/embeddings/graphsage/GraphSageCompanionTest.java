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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.core.Username;
import org.neo4j.gds.core.model.InjectModelCatalog;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.model.ModelCatalogExtension;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSage;
import org.neo4j.gds.embeddings.graphsage.algo.ImmutableGraphSageTrainConfig;

import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.gds.config.RelationshipWeightConfig.RELATIONSHIP_WEIGHT_PROPERTY;
import static org.neo4j.gds.model.ModelConfig.MODEL_NAME_KEY;

@ModelCatalogExtension
class GraphSageCompanionTest {

    static final String USER_NAME = Username.EMPTY_USERNAME.username();

    @InjectModelCatalog
    ModelCatalog modelCatalog;

    @BeforeEach
    void beforeAll() {
        var unweightedModel = Model.of(
            USER_NAME,
            "myModel",
            GraphSage.MODEL_TYPE,
            GraphSchema.empty(),
            ModelData.of(new Layer[0], new SingleLabelFeatureFunction()),
            ImmutableGraphSageTrainConfig
                .builder()
                .modelName("myWeightedModel")
                .addFeatureProperties("a")
                .build(),
            GraphSageModelTrainer.GraphSageTrainMetrics.empty()
        );

        modelCatalog.set(unweightedModel);

        var weightedModel = Model.of(
            USER_NAME,
            "myWeightedModel",
            GraphSage.MODEL_TYPE,
            GraphSchema.empty(),
            ModelData.of(new Layer[0], new SingleLabelFeatureFunction()),
            ImmutableGraphSageTrainConfig
                .builder()
                .modelName("myWeightedModel")
                .relationshipWeightProperty("myWeight")
                .addFeatureProperties("a")
                .build(),
            GraphSageModelTrainer.GraphSageTrainMetrics.empty()
        );

        modelCatalog.set(weightedModel);
    }

    @Test
    void injectRelWeight() {
        var userConfig = new HashMap<String, Object>();
        userConfig.put(MODEL_NAME_KEY, "myWeightedModel");

        GraphSageCompanion.injectRelationshipWeightPropertyFromModel(userConfig, modelCatalog, USER_NAME);

        assertThat(userConfig).containsEntry(RELATIONSHIP_WEIGHT_PROPERTY, "myWeight");
    }

    @Test
    void injectNullRelWeightForUnweightedModel() {
        var userConfig = new HashMap<String, Object>();
        userConfig.put(MODEL_NAME_KEY, "myModel");

        GraphSageCompanion.injectRelationshipWeightPropertyFromModel(userConfig, modelCatalog, USER_NAME);

        assertThat(userConfig).containsEntry(RELATIONSHIP_WEIGHT_PROPERTY, null);
    }

    @Test
    void failInjectRelWeightIfModelNameMissing() {
        var userConfig = new HashMap<String, Object>();

        assertThatThrownBy(() -> GraphSageCompanion.injectRelationshipWeightPropertyFromModel(userConfig, modelCatalog, Username.EMPTY_USERNAME.username()))
            .hasMessage("No value specified for the mandatory configuration parameter `modelName`");
    }

    @Test
    void failInjectRelWeightIfAlreadyPresent() {
        var userConfig = new HashMap<String, Object>();
        userConfig.put(MODEL_NAME_KEY, "myModel");
        userConfig.put(RELATIONSHIP_WEIGHT_PROPERTY, "otherWeight");

        assertThatThrownBy(() -> GraphSageCompanion.injectRelationshipWeightPropertyFromModel(userConfig, modelCatalog, Username.EMPTY_USERNAME.username()))
            .hasMessage("The parameter `relationshipWeightProperty` cannot be overwritten during embedding computation. Instead, specify this parameter in the configuration of the model training.");
    }
}
