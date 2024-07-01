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
package org.neo4j.gds.compat;

import org.jetbrains.annotations.NotNull;

public sealed interface Neo4jVersion {

    static Neo4jVersion findNeo4jVersion() {
        return Neo4jVersionLookup.neo4jVersion();
    }

    int major();

    int minor();

    String fullVersion();

    default MajorMinor semanticVersion() {
        return new MajorMinor(this.major(), this.minor());
    }

    default boolean matches(int major, int minor) {
        return this.major() == major && this.minor() == minor;
    }

    default boolean matches(@NotNull MajorMinor version) {
        return this.major() == version.major() && this.minor() == version.minor();
    }

    default boolean isSupported() {
        return !(this instanceof Unsupported);
    }

    record MajorMinor(int major, int minor) {
        @Override
        public String toString() {
            return this.major + "." + this.minor;
        }
    }

    record Known(int major, int minor, String fullVersion) implements Neo4jVersion {
        @Override
        public String toString() {
            return this.major + "." + this.minor;
        }
    }

    record Unsupported(int major, int minor, String fullVersion) implements Neo4jVersion {
        @Override
        public String toString() {
            if (this.major >= 0 && this.minor >= 0) {
                return this.major + "." + this.minor + " (" + this.fullVersion + ")";
            } else {
                return this.fullVersion;
            }
        }
    }
}
