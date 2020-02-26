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
package org.neo4j.graphalgo.wcc;

import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.WRITE;
import static org.neo4j.procedure.Mode.WRITE;

public class WccWriteProc extends WccBaseProc<WccWriteConfig> {

    @Procedure(value = "gds.wcc.write", mode = WRITE)
    @Description(WCC_DESCRIPTION)
    public Stream<WriteResult> write(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<Wcc, DisjointSetStruct, WccWriteConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );
        return write(computationResult);
    }

    @Procedure(value = "gds.wcc.write.estimate", mode = WRITE)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> writeEstimate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    protected WccWriteConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return WccWriteConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected PropertyTranslator<DisjointSetStruct> nodePropertyTranslator(
        ComputationResult<Wcc, DisjointSetStruct, WccWriteConfig> computationResult
    ) {
        WccWriteConfig config = computationResult.config();

        boolean consecutiveIds = config.consecutiveIds();
        boolean isIncremental = config.isIncremental();
        boolean seedPropertyEqualsWriteProperty = config.writeProperty().equalsIgnoreCase(config.seedProperty());

        PropertyTranslator<DisjointSetStruct> propertyTranslator;
        if (seedPropertyEqualsWriteProperty && !consecutiveIds) {
            NodeProperties seedProperties = computationResult.graph().nodeProperties(config.seedProperty());
            propertyTranslator = new PropertyTranslator.OfLongIfChanged<>(seedProperties, DisjointSetStruct::setIdOf);
        } else if (consecutiveIds && !isIncremental) {
            propertyTranslator = new ConsecutivePropertyTranslator(
                computationResult.result(),
                computationResult.tracker()
            );
        } else {
            propertyTranslator = (PropertyTranslator.OfLong<DisjointSetStruct>) DisjointSetStruct::setIdOf;
        }

        return propertyTranslator;
    }
}
