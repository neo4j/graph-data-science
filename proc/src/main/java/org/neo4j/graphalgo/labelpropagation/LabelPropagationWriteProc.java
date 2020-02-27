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
package org.neo4j.graphalgo.labelpropagation;

import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

public class LabelPropagationWriteProc extends LabelPropagationBaseProc<LabelPropagationWriteConfig> {

    @Procedure(value = "gds.labelPropagation.write", mode = WRITE)
    @Description(LABEL_PROPAGATION_DESCRIPTION)
    public Stream<WriteResult> write(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<LabelPropagation, LabelPropagation, LabelPropagationWriteConfig> result = compute(
            graphNameOrConfig,
            configuration
        );
        return write(result);
    }

    @Procedure(value = "gds.labelPropagation.write.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    protected PropertyTranslator<LabelPropagation> nodePropertyTranslator(ComputationResult<LabelPropagation, LabelPropagation, LabelPropagationWriteConfig> computationResult) {

        LabelPropagationWriteConfig config = computationResult.config();

        boolean writePropertyEqualsSeedProperty = config.writeProperty().equals(config.seedProperty());
        NodeProperties seedProperties = computationResult.graph().nodeProperties(config.seedProperty());

        if (writePropertyEqualsSeedProperty) {
            return new PropertyTranslator.OfLongIfChanged<>(
                seedProperties,
                (data, nodeId) -> data.labels().get(nodeId)
            );
        }

        return (PropertyTranslator.OfLong<LabelPropagation>) (data, nodeId) -> data
            .labels()
            .get(nodeId);
    }

    @Override
    public LabelPropagationWriteConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return LabelPropagationWriteConfig.of(username, graphName, maybeImplicitCreate, config);
    }
}
