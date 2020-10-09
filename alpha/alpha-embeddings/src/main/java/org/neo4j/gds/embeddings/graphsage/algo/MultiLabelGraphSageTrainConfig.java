/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.gds.embeddings.graphsage.algo;

import org.immutables.value.Value;
import org.neo4j.gds.embeddings.graphsage.LayerConfig;
import org.neo4j.graphalgo.annotation.Configuration;
import org.neo4j.graphalgo.annotation.ValueClass;

import java.util.ArrayList;
import java.util.List;

@ValueClass
@Configuration("MultiLabelGraphSageTrainConfigImpl")
@SuppressWarnings("immutables:subtype")
public interface MultiLabelGraphSageTrainConfig extends GraphSageTrainConfig {

    int PROJECTED_FEATURE_SIZE = 5;

    @Configuration.Ignore
    @Override
    default List<LayerConfig> layerConfigs() {
        List<LayerConfig> result = new ArrayList<>(sampleSizes().size());
        for (int i = 0; i < sampleSizes().size(); i++) {
            LayerConfig layerConfig = LayerConfig.builder()
                .aggregatorType(aggregator())
                .activationFunction(activationFunction())
                .rows(embeddingDimension())
                .cols(i == 0 ? projectedFeatureSize() : embeddingDimension())
                .sampleSize(sampleSizes().get(i))
                .build();

            result.add(layerConfig);
        }
        return result;
    }

    @Value.Default
    default int projectedFeatureSize() {
        return PROJECTED_FEATURE_SIZE;
    }

}
