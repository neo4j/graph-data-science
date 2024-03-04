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
package org.neo4j.gds.embeddings.graphsage.algo;

import org.neo4j.gds.embeddings.graphsage.LayerConfig;

import java.util.List;

public class GraphSageTrainMemoryEstimateParameters {

    private final List<LayerConfig> layerConfigs;
    private final boolean isMultiLabel;
    private final int numberOfFeatureProperties;
    private final int estimationFeatureDimension;
    private final int batchSize;
    private final int embeddingDimension;

    GraphSageTrainMemoryEstimateParameters(
        List<LayerConfig> layerConfigs,
        boolean isMultiLabel,
        int numberOfFeatureProperties,
        int estimationFeatureDimension,
        int batchSize,
        int embeddingDimension
    ) {
        this.layerConfigs = layerConfigs;
        this.isMultiLabel = isMultiLabel;
        this.numberOfFeatureProperties = numberOfFeatureProperties;
        this.estimationFeatureDimension = estimationFeatureDimension;
        this.batchSize = batchSize;
        this.embeddingDimension = embeddingDimension;
    }

    public List<LayerConfig> layerConfigs() {
        return layerConfigs;
    }

    public boolean isMultiLabel() {
        return isMultiLabel;
    }

    int numberOfFeatureProperties() {
        return numberOfFeatureProperties;
    }

    public int estimationFeatureDimension() {
        return estimationFeatureDimension;
    }

    int batchSize() {
        return batchSize;
    }

    public int embeddingDimension() {
        return embeddingDimension;
    }
}
