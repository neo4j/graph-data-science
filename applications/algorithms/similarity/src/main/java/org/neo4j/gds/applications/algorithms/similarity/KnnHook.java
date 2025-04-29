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
package org.neo4j.gds.applications.algorithms.similarity;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.loading.PostLoadValidationHook;
import org.neo4j.gds.similarity.knn.KnnNodePropertySpec;
import org.neo4j.gds.utils.StringJoining;

import java.util.List;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class KnnHook  implements PostLoadValidationHook {

    private List<KnnNodePropertySpec> knnNodeProperties;

    public KnnHook(List<KnnNodePropertySpec> knnNodeProperties) {this.knnNodeProperties = knnNodeProperties;}

    @Override
    public void onGraphStoreLoaded(GraphStore graphStore) {
        //ignore me
    }

    @Override
    public void onGraphLoaded(Graph graph) {
        for (var property : knnNodeProperties){
            var propertyName = property.name();
            if (graph.nodeProperties(propertyName) == null){
                throw  new IllegalArgumentException(
                     formatWithLocale("The property `%s` has not been loaded. Available properties: %s",
                         propertyName,
                         StringJoining.join(graph.availableNodeProperties()))
                );
            }

        }
    }
}
