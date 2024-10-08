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
package org.neo4j.gds.kmeans;

import org.neo4j.gds.utils.StringFormatting;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

public enum SamplerType {
        UNIFORM("UNIFORM", "UNIFORM"), KMEANSPP("KMEANS++", "KMEANSPP");

        private final String samplerName;
        private final String samplerType;

        SamplerType(String samplerName, String samplerType) {
            this.samplerName = samplerName;
            this.samplerType = samplerType;
        }

        public String getSamplerName() {
            return this.samplerName;
        }

        public String getSamplerType() {
            return this.samplerType;
        }


        private static final Map<String, String> VALUES = Arrays
            .stream(SamplerType.values())
            .collect(Collectors.toMap(SamplerType::getSamplerName, SamplerType::getSamplerType));

        public static SamplerType parse(Object input) {
            if (input instanceof String) {
                var inputString = StringFormatting.toUpperCaseWithLocale((String) input);

                if (VALUES.containsKey(inputString)) {
                    return SamplerType.valueOf(VALUES.get(inputString));
                }

                throw new IllegalArgumentException(String.format(
                    Locale.getDefault(),
                    "Sampler `%s` is not supported. Must be one of: %s.",
                    inputString,
                    VALUES
                ));
            } else if (input instanceof SamplerType) {
                return (SamplerType) input;
            }

            throw new IllegalArgumentException(StringFormatting.formatWithLocale(
                "Expected Sampler or String. Got %s.",
                input.getClass().getSimpleName()
            ));
        }

        public static String toString(SamplerType samplerType) {
            return samplerType.toString();
        }

    }
