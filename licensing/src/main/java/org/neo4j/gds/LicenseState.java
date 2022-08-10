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
import java.util.Optional;

public interface LicenseState {

    <R, P> R visit(VisitorWithParameter<R, P> visitor, P parameter);

    default <R> R visit(Visitor<R> visitor) {
        return visit(visitor, null);
    }

    default boolean isLicensed() {
        return visit(Check.LICENSED);
    }

    interface VisitorWithParameter<R, P> {
        R unlicensed(String name, P parameter);

        R licensed(String name, ZonedDateTime expirationTime, P parameter);

        R invalid(String name, String errorMessage, Optional<ZonedDateTime> expirationTime, P parameter);
    }

    interface Visitor<R> extends VisitorWithParameter<R, Void> {
        R unlicensed(String name);

        R licensed(String name, ZonedDateTime expirationTime);

        R invalid(String name, String errorMessage, Optional<ZonedDateTime> expirationTime);

        @Override
        default R unlicensed(String name, Void parameter) {
            return unlicensed(name);
        }

        @Override
        default R licensed(String name, ZonedDateTime expirationTime, Void parameter) {
            return licensed(name, expirationTime);
        }

        @Override
        default R invalid(String name, String errorMessage, Optional<ZonedDateTime> expirationTime, Void parameter) {
            return invalid(name, errorMessage, expirationTime);
        }
    }

    enum Check implements LicenseState.Visitor<Boolean> {
        LICENSED {
            @Override
            public Boolean unlicensed(String name) {
                return false;
            }

            @Override
            public Boolean licensed(String name, ZonedDateTime expirationTime) {
                return true;
            }

            @Override
            public Boolean invalid(String name, String errorMessage, Optional<ZonedDateTime> expirationTime) {
                return false;
            }
        },
    }
}
