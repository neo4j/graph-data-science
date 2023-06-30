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
package org.neo4j.gds;

import java.time.ZonedDateTime;
import java.util.Locale;
import java.util.Optional;

public final class LicenseDetails {
    private final boolean isLicensed;
    private final String details;

    private LicenseDetails(boolean isLicensed, String details) {
        this.isLicensed = isLicensed;
        this.details = details;
    }

    static LicenseDetails from(LicenseState licenseState) {
        String details = licenseState.visit(StateDetailsExtractor.INSTANCE);
        return new LicenseDetails(licenseState.isLicensed(), details);
    }

    String details() {
        return details;
    }

    boolean isLicensed() {
        return isLicensed;
    }

    private enum StateDetailsExtractor implements LicenseState.Visitor<String> {
        INSTANCE;

        @Override
        public String unlicensed(String name) {
            return "No valid GDS license specified.";
        }

        @Override
        public String licensed(String name, ZonedDateTime expirationTime) {
            return String.format(Locale.US, "License expires at %s", expirationTime);
        }

        @Override
        public String invalid(
            String name,
            String errorMessage,
            Optional<ZonedDateTime> expirationTime
        ) {
            Optional<String> expatriationDate = expirationTime
                .map(expiration -> " Expiration date at " + licensed(name, expiration));

            return "License error: " + errorMessage + expatriationDate.orElse("");
        }
    }
}
