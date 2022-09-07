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

import org.neo4j.gds.ElementProjection;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.ElementTypeValidator;
import org.neo4j.gds.config.GraphNameConfig;
import org.neo4j.gds.model.ModelConfig;

import java.util.Collection;
import java.util.List;

@Configuration
public interface NodePropertyPredictPipelineBaseConfig extends
    AlgoBaseConfig,
    GraphNameConfig,
    ModelConfig {


    default List<String> targetNodeLabels() {return List.of();}

    @Override
    default List<String> relationshipTypes() {
        return List.of();
    }

    @Override
    @Configuration.Ignore
    default List<String> nodeLabels() {
        // The graph is derived manually in the algo factory.
        return List.of(ElementProjection.PROJECT_ALL);
    }

    @Override
    @Configuration.Ignore
    default Collection<NodeLabel> nodeLabelIdentifiers(GraphStore graphStore) {
        return ElementTypeValidator.resolve(graphStore, targetNodeLabels());
    }

}
