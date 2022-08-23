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
package org.neo4j.gds.ml.pipeline.nodePipeline.classification.train;

import org.neo4j.gds.ElementProjection;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.collections.LongMultiSet;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.GraphNameConfig;
import org.neo4j.gds.config.RandomSeedConfig;
import org.neo4j.gds.config.TargetNodePropertyConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;
import org.neo4j.gds.ml.metrics.Metric;
import org.neo4j.gds.ml.metrics.classification.ClassificationMetric;
import org.neo4j.gds.ml.metrics.classification.ClassificationMetricSpecification;
import org.neo4j.gds.model.ModelConfig;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Configuration
@SuppressWarnings("immutables:subtype")
public interface NodeClassificationPipelineTrainConfig extends AlgoBaseConfig, GraphNameConfig, ModelConfig, RandomSeedConfig, TargetNodePropertyConfig {

    long serialVersionUID = 0x42L;

    @Configuration.ConvertWith("org.neo4j.gds.ml.metrics.classification.ClassificationMetricSpecification.Parser#parse")
    @Configuration.ToMapValue("org.neo4j.gds.ml.metrics.classification.ClassificationMetricSpecification#specificationsToString")
    List<ClassificationMetricSpecification> metrics();

    String pipeline();

    default List<String> targetNodeLabels() { return List.of(ElementProjection.PROJECT_ALL); }

    default List<String> contextNodeLabels() { return List.of(); }

    @Configuration.Ignore
    default List<Metric> metrics(LocalIdMap classIdMap,  LongMultiSet classCounts) {
        return metrics()
            .stream()
            .flatMap(spec -> spec.createMetrics(classIdMap, classCounts))
            .collect(Collectors.toList());
    }

    static List<ClassificationMetric> classificationMetrics(List<Metric> metrics) {
        return metrics
            .stream()
            .filter(metric -> !metric.isModelSpecific())
            .map(metric -> (ClassificationMetric) metric)
            .collect(Collectors.toList());
    }

    static NodeClassificationPipelineTrainConfig of(String username, CypherMapWrapper config) {
        return new NodeClassificationPipelineTrainConfigImpl(username, config);
    }

    @Configuration.Ignore
    default Collection<NodeLabel> featureInputLabels(GraphStore graphStore) {
        return contextNodeLabels().contains(ElementProjection.PROJECT_ALL)
            ? graphStore.nodeLabels()
            : Stream.concat(contextNodeLabels().stream().map(NodeLabel::of), nodeLabelIdentifiers(graphStore).stream())
                .collect(Collectors.toSet());
    }

    @Override
    @Configuration.Ignore
    default List<String> nodeLabels() {
        return targetNodeLabels();
    }

}
