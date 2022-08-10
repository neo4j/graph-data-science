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
package org.neo4j.gds.configuration;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class LimitsConfiguration {
    private final Map<String, Limit> globalLimits;
    private final Map<String, Map<String, Limit>> personalLimits;

    public LimitsConfiguration(Map<String, Limit> globalLimits, Map<String, Map<String, Limit>> personalLimits) {
        this.globalLimits = globalLimits;
        this.personalLimits = personalLimits;
    }

    public Set<LimitViolation> validate(Map<String, Object> configuration, String username) {
        var limitViolations = new HashSet<LimitViolation>();

        for (Map.Entry<String, Object> inputParameter : configuration.entrySet()) {
            var key = inputParameter.getKey();
            var value = inputParameter.getValue();

            // personal limits take precedence
            if (personalLimits.getOrDefault(username, Collections.emptyMap()).containsKey(key)) {
                Limit limit = personalLimits.get(username).get(key);

                if (limit.isViolated(value)) {
                    var limitViolation = new LimitViolation(
                        key,
                        String.valueOf(value),
                        limit.getValueAsString()
                    );

                    limitViolations.add(limitViolation);

                    continue; // we found a violation; skip to next parameter
                }
            }

            // global limits come second
            if (!globalLimits.containsKey(key)) continue;

            var limit = globalLimits.get(key);

            if (limit.isViolated(value)) {
                var limitViolation = new LimitViolation(
                    key,
                    String.valueOf(value),
                    limit.getValueAsString()
                );

                limitViolations.add(limitViolation);
            }
        }

        return limitViolations;
    }
}
