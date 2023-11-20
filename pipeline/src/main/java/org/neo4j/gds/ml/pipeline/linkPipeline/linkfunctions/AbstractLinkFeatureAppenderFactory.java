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

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.ml.pipeline.FeatureStepUtil;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkFeatureAppender;

import java.util.List;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

abstract class AbstractLinkFeatureAppenderFactory {

    protected abstract LinkFeatureAppender doubleArrayAppender(NodePropertyValues props, int dimension);
    protected abstract LinkFeatureAppender floatArrayAppender(NodePropertyValues props, int dimension);
    protected abstract LinkFeatureAppender longArrayAppender(NodePropertyValues props, int dimension);
    protected abstract LinkFeatureAppender longAppender(NodePropertyValues props, int dimension);
    protected abstract LinkFeatureAppender doubleAppender(NodePropertyValues props, int dimension);

    private LinkFeatureAppender createAppender(Graph graph, String propertyName) {
        var props = graph.nodeProperties(propertyName);
        var propertyType = props.valueType();

        var dimension = FeatureStepUtil.propertyDimension(graph, propertyName);

        switch (propertyType) {
            case DOUBLE_ARRAY:
                return doubleArrayAppender(props, dimension);
            case FLOAT_ARRAY:
                return floatArrayAppender(props, dimension);
            case LONG_ARRAY:
                return longArrayAppender(props, dimension);
            case LONG:
                return longAppender(props, dimension);
            case DOUBLE:
                return doubleAppender(props, dimension);
            default:
                throw new IllegalStateException(formatWithLocale("Unsupported ValueType %s", propertyType));
        }
    }

    LinkFeatureAppender[] createAppenders(Graph graph, List<String> propertyNames) {
        var appenderPerProperty = new LinkFeatureAppender[propertyNames.size()];
        for (int idx = 0, nodePropertiesSize = propertyNames.size(); idx < nodePropertiesSize; idx++) {
            appenderPerProperty[idx] = createAppender(graph, propertyNames.get(idx));
        }

        return appenderPerProperty;
    }
}
