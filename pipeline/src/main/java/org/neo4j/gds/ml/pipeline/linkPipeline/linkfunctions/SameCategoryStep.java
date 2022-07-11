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
package org.neo4j.gds.ml.pipeline.linkPipeline.linkfunctions;

import com.carrotsearch.hppc.predicates.LongLongPredicate;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.properties.nodes.NodePropertyContainer;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkFeatureAppender;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkFeatureStep;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkFeatureStepFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class SameCategoryStep implements LinkFeatureStep {


    private final List<String> nodeProperties;

    public SameCategoryStep(List<String> nodeProperties) {

        this.nodeProperties = nodeProperties;
    }

    @Override
    public List<String> inputNodeProperties() {
        return nodeProperties;
    }

    @Override
    public String name() {
        return LinkFeatureStepFactory.SAME_CATEGORY.name();
    }

    @Override
    public Map<String, Object> configuration() {
        return Map.of("nodeProperty", nodeProperties);
    }

    @Override
    public LinkFeatureAppender linkFeatureAppender(Graph graph) {
        var isSamePredicates = new ArrayList<LongLongPredicate>();

        for (String nodeProperty : nodeProperties) {
            isSamePredicates.add(sameCategoryPredicate(graph, nodeProperty));
        }

        return new LinkFeatureAppender() {
            @Override
            public void appendFeatures(long source, long target, double[] linkFeatures, int offset) {
                int localOffset = offset;
                for (LongLongPredicate predicate : isSamePredicates) {
                    linkFeatures[localOffset++] = predicate.apply(source, target) ? 1.0 : 0.0;
                }
            }

            @Override
            public int dimension() {
                return nodeProperties.size();
            }
        };
    }

    private LongLongPredicate sameCategoryPredicate(NodePropertyContainer graph, String nodeProperty) {
        var propertyValues = graph.nodeProperties(nodeProperty);

        switch (propertyValues.valueType()) {
            case LONG:
                return (source, target) -> propertyValues.longValue(source) == propertyValues.longValue(target);
            case DOUBLE:
                return (source, target) -> propertyValues.doubleValue(source) == propertyValues.doubleValue(target);
            default:
                throw new IllegalArgumentException(formatWithLocale("%s only supports combining numeric properties, but got node property `%s` of type %s.", name(), nodeProperty, propertyValues.valueType()));
        }
    }
}
