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
import org.neo4j.gds.core.CypherMapWrapper;

@Generated("org.neo4j.gds.proc.ConfigurationProcessor")
public final class ParametersConfig implements Parameters {
    private int keyFromParameter;

    private long keyFromMap;

    private int parametersAreAddedFirst;

    public ParametersConfig(
        int keyFromParameter, int parametersAreAddedFirst,
        @NotNull CypherMapWrapper config
    ) {
        ArrayList<IllegalArgumentException> errors = new ArrayList<>();
        try {
            this.keyFromParameter = keyFromParameter;
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.keyFromMap = config.requireLong("keyFromMap");
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.parametersAreAddedFirst = parametersAreAddedFirst;
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
    public int keyFromParameter() {
        return this.keyFromParameter;
    }

    @Override
    public long keyFromMap() {
        return this.keyFromMap;
    }

    @Override
    public int parametersAreAddedFirst() {
        return this.parametersAreAddedFirst;
    }

    public static ParametersConfig.Builder builder() {
        return new ParametersConfig.Builder();
    }

    public static final class Builder {
        private final Map<String, Object> config;

        private int keyFromParameter;

        private int parametersAreAddedFirst;

        public Builder() {
            this.config = new HashMap<>();
        }

        public static ParametersConfig.Builder from(Parameters baseConfig) {
            var builder = new ParametersConfig.Builder();
            builder.keyFromParameter(baseConfig.keyFromParameter());
            builder.keyFromMap(baseConfig.keyFromMap());
            builder.parametersAreAddedFirst(baseConfig.parametersAreAddedFirst());
            return builder;
        }

        public ParametersConfig.Builder keyFromParameter(int keyFromParameter) {
            this.keyFromParameter = keyFromParameter;
            return this;
        }

        public ParametersConfig.Builder parametersAreAddedFirst(int parametersAreAddedFirst) {
            this.parametersAreAddedFirst = parametersAreAddedFirst;
            return this;
        }

        public ParametersConfig.Builder keyFromMap(long keyFromMap) {
            this.config.put("keyFromMap", keyFromMap);
            return this;
        }

        public Parameters build() {
            CypherMapWrapper config = CypherMapWrapper.create(this.config);
            return new ParametersConfig(keyFromParameter, parametersAreAddedFirst, config);
        }
    }
}
