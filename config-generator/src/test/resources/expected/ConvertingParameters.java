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
package positive;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.processing.Generated;
import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.core.CypherMapAccess;
import org.neo4j.gds.core.CypherMapWrapper;

@Generated("org.neo4j.gds.proc.ConfigurationProcessor")
public final class ConvertingParametersConfig implements ConvertingParameters {
    private int parametersAreSubjectToConversion;

    public ConvertingParametersConfig(@NotNull String parametersAreSubjectToConversion) {
        ArrayList<IllegalArgumentException> errors = new ArrayList<>();
        try {
            this.parametersAreSubjectToConversion = ConvertingParameters.toInt(CypherMapAccess.failOnNull(
                "parametersAreSubjectToConversion",
                parametersAreSubjectToConversion
            ));
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        if (!errors.isEmpty()) {
            if (errors.size() == 1) {
                throw errors.get(0);
            } else {
                String combinedErrorMsg = errors
                    .stream()
                    .map(IllegalArgumentException::getMessage)
                    .collect(Collectors.joining(System.lineSeparator() + "\t\t\t\t",
                        "Multiple errors in configuration arguments:" + System.lineSeparator() + "\t\t\t\t",
                        ""
                    ));
                IllegalArgumentException combinedError = new IllegalArgumentException(combinedErrorMsg);
                errors.forEach(error -> combinedError.addSuppressed(error));
                throw combinedError;
            }
        }
    }

    @Override
    public int parametersAreSubjectToConversion() {
        return this.parametersAreSubjectToConversion;
    }

    public static ConvertingParametersConfig.Builder builder() {
        return new ConvertingParametersConfig.Builder();
    }

    public static final class Builder {
        private final Map<String, Object> config;

        private @NotNull String parametersAreSubjectToConversion;

        public Builder() {
            this.config = new HashMap<>();
        }

        public static ConvertingParametersConfig.Builder from(ConvertingParameters baseConfig) {
            var builder = new ConvertingParametersConfig.Builder();
            builder.parametersAreSubjectToConversion(String.valueOf(baseConfig.parametersAreSubjectToConversion()));
            return builder;
        }

        public ConvertingParametersConfig.Builder parametersAreSubjectToConversion(String parametersAreSubjectToConversion) {
            this.parametersAreSubjectToConversion = parametersAreSubjectToConversion;
            return this;
        }

        public ConvertingParameters build() {
            CypherMapWrapper config = CypherMapWrapper.create(this.config);
            return new ConvertingParametersConfig(parametersAreSubjectToConversion);
        }
    }
}
