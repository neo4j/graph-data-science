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

import org.jetbrains.annotations.VisibleForTesting;
import org.neo4j.kernel.internal.Version;

import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

final class Neo4jVersionLookup {
    private static final Neo4jVersion VERSION = findNeo4jVersion();

    private Neo4jVersionLookup() {}

    static Neo4jVersion neo4jVersion() {
        return Neo4jVersionLookup.VERSION;
    }

    @VisibleForTesting
    static Neo4jVersion findNeo4jVersion() {
        var neo4jVersion = Objects.requireNonNullElse(Version.class.getPackage().getImplementationVersion(), "Unknown");
        // some versions have a build thing attached at the end
        // e.g. 4.0.8,8e921029f7daebacc749034f0cb174f1f2c7a258
        // This regex follows the logic from org.neo4j.kernel.internal.Version.parseReleaseVersion
        Pattern pattern = Pattern.compile(
            "(\\d+" +                  // Major version
                "\\.\\d+" +                // Minor version
                "(\\.\\d+)?" +             // Optional patch version
                "(-?[^,]+)?)" +            // Optional marker, like M01, GA, SNAPSHOT - anything other than a comma
                ".*"                       // Anything else, such as git revision
        );
        var matcher = pattern.matcher(neo4jVersion);
        // If no match is found, use the fullVersion version.
        var minorMajor = matcher.find() ? matcher.group(1) : neo4jVersion;
        return parse(minorMajor, neo4jVersion);
    }

    private static final int SUPPORTED_MAJOR_VERSION = 5;
    private static final int MIN_SUPPORTED_MINOR_VERSION = 24;

    @VisibleForTesting
    static Neo4jVersion parse(CharSequence version, String fullVersion) {
        var versionSegments = Pattern.compile("[.-]")
            .splitAsStream(version)
            .mapToInt(v -> {
                try {
                    return Integer.parseInt(v);
                } catch (NumberFormatException notANumber) {
                    return -1;
                }
            });

        var majorMinorVersion = IntStream.concat(versionSegments, IntStream.of(-1, -1))
            .limit(2)
            .toArray();

        var majorVersion = majorMinorVersion[0];
        var minorVersion = majorMinorVersion[1];

        if (majorVersion != SUPPORTED_MAJOR_VERSION || minorVersion < MIN_SUPPORTED_MINOR_VERSION) {
            return new Neo4jVersion.Unsupported(majorVersion, minorVersion, fullVersion);
        }

        return new Neo4jVersion.Known(majorVersion, minorVersion, fullVersion);
    }
}
