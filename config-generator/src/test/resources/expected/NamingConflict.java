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
public final class NamingConflictConfig implements NamingConflict {
    private int config;

    private int anotherConfig;

    private int config_;

    public NamingConflictConfig(int config_, @NotNull CypherMapAccess config__) {
        ArrayList<IllegalArgumentException> errors = new ArrayList<>();
        try {
            this.config = config__.requireInt("config");
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.anotherConfig = config__.requireInt("config");
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.config_ = config_;
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
    public int config() {
        return this.config;
    }

    @Override
    public int anotherConfig() {
        return this.anotherConfig;
    }

    @Override
    public int config_() {
        return this.config_;
    }

    public static NamingConflictConfig.Builder builder() {
        return new NamingConflictConfig.Builder();
    }

    public static final class Builder {
        private final Map<String, Object> config__;

        private int config_;

        public Builder() {
            this.config__ = new HashMap<>();
        }

        public static NamingConflictConfig.Builder from(NamingConflict baseConfig) {
            var builder = new NamingConflictConfig.Builder();
            builder.config(baseConfig.config());
            builder.anotherConfig(baseConfig.anotherConfig());
            builder.config_(baseConfig.config_());
            return builder;
        }

        public NamingConflictConfig.Builder config_(int config_) {
            this.config_ = config_;
            return this;
        }

        public NamingConflictConfig.Builder config(int config) {
            this.config__.put("config", config);
            return this;
        }

        public NamingConflictConfig.Builder anotherConfig(int anotherConfig) {
            this.config__.put("config", anotherConfig);
            return this;
        }

        public NamingConflict build() {
            CypherMapWrapper config__ = CypherMapWrapper.create(this.config__);
            return new NamingConflictConfig(config_, config__);
        }
    }
}
