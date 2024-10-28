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
package org.neo4j.gds.procedures.pipelines;

import org.neo4j.gds.ml.pipeline.nodePipeline.NodeFeatureStep;
import org.neo4j.gds.utils.StringFormatting;

import java.util.ArrayList;
import java.util.List;

class NodeFeatureStepsParser {
    List<NodeFeatureStep> parse(Object nodeFeatureStepsSpecification, String label) {
        if (nodeFeatureStepsSpecification instanceof String)
            return List.of(NodeFeatureStep.of((String) nodeFeatureStepsSpecification));

        if (nodeFeatureStepsSpecification instanceof List) {
            //noinspection rawtypes
            var propertiesList = (List) nodeFeatureStepsSpecification;

            var nodeFeatureSteps = new ArrayList<NodeFeatureStep>(propertiesList.size());

            for (Object o : propertiesList) {
                if (o instanceof String)
                    nodeFeatureSteps.add(NodeFeatureStep.of((String) o));
                else
                    throw new IllegalArgumentException(StringFormatting.formatWithLocale(
                        "The list `%s` is required to contain only strings.",
                        label
                    ));
            }

            return nodeFeatureSteps;
        }

        throw new IllegalArgumentException(StringFormatting.formatWithLocale(
            "The value of `%s` is required to be a list of strings.",
            label
        ));
    }
}
