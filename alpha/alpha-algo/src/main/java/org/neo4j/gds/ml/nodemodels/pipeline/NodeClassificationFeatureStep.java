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
package org.neo4j.gds.ml.nodemodels.pipeline;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.config.ToMapConvertible;
import org.neo4j.gds.ml.pipeline.FeatureStep;

import java.util.List;
import java.util.Map;

public class NodeClassificationFeatureStep implements ToMapConvertible, FeatureStep {

    private final String nodeProperty;

    NodeClassificationFeatureStep(String nodeProperty) {
        this.nodeProperty = nodeProperty;
    }

    @Override
    public List<String> inputNodeProperties() {
        return List.of(nodeProperty);
    }

    @Override
    public String name() {
        //TODO move this method to LinkFeatureStep
        throw new IllegalStateException("NodeClassificationFeatureStep does not have name");
    }

    @Override
    public Map<String, Object> configuration() {
        //TODO move this method to LinkFeatureStep
        throw new IllegalStateException("NodeClassificationFeatureStep does not have configuration");
    }

    @Override
    public int outputFeatureDimension(Graph graph) {
        //TODO move this method to LinkFeatureStep
        throw new IllegalStateException("NodeClassificationFeatureStep does not support outputFeatureDimension");
    }

    @Override
    public Map<String, Object> toMap() {
        return Map.of("feature", nodeProperty);
    }
}
