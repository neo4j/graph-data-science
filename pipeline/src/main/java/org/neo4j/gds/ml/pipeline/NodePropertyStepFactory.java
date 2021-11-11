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
package org.neo4j.gds.ml.pipeline;

import org.neo4j.gds.BaseProc;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.ml.pipeline.proc.ProcedureReflection;
import org.neo4j.gds.utils.StringJoining;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class NodePropertyStepFactory {
    private static final List<String> RESERVED_CONFIG_KEYS = List.of(AlgoBaseConfig.NODE_LABELS_KEY, AlgoBaseConfig.RELATIONSHIP_TYPES_KEY);

    public static NodePropertyStep createNodePropertyStep(BaseProc caller, String taskName, Map<String, Object> procedureConfig) {
        validateReservedConfigKeys(procedureConfig);
        validateConfig(caller, taskName, procedureConfig);
        return NodePropertyStep.of(taskName, procedureConfig);
    }

    private static void validateConfig(BaseProc caller, String taskName, Map<String, Object> procedureConfig) {
        var wrappedConfig = CypherMapWrapper.create(procedureConfig);
        var procedureMethod = ProcedureReflection.INSTANCE.findProcedureMethod(taskName);
        Optional<AlgoBaseConfig> typedConfig = ProcedureReflection.INSTANCE.createAlgoConfig(
            caller,
            procedureMethod,
            wrappedConfig
        );
        typedConfig.ifPresent(config -> wrappedConfig.requireOnlyKeysFrom(config.configKeys()));
    }

    public static void validateReservedConfigKeys(Map<String, Object> procedureConfig) {
        if (RESERVED_CONFIG_KEYS.stream().anyMatch(procedureConfig::containsKey)) {
            throw new IllegalArgumentException(formatWithLocale(
                "Cannot configure %s for an individual node property step, but can only be configured at `train` and `predict` mode.",
                StringJoining.join(RESERVED_CONFIG_KEYS)
            ));
        }
    }
}
