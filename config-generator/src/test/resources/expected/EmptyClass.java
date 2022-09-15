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

import java.util.HashMap;
import java.util.Map;
import javax.annotation.processing.Generated;
import org.neo4j.gds.core.CypherMapWrapper;

@Generated("org.neo4j.gds.proc.ConfigurationProcessor")
public final class EmptyClassConfig implements EmptyClass {

    public EmptyClassConfig() {
    }

    public static EmptyClassConfig.Builder builder() {
        return new EmptyClassConfig.Builder();
    }

    public static final class Builder {
        private final Map<String, Object> config;

        public Builder() {
            this.config = new HashMap<>();
        }

        public static EmptyClassConfig.Builder from(EmptyClass baseConfig) {
            var builder = new EmptyClassConfig.Builder();
            return builder;
        }

        public EmptyClass build() {
            CypherMapWrapper config = CypherMapWrapper.create(this.config);
            return new EmptyClassConfig();
        }
    }
}
