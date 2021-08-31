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
package org.neo4j.gds.core;

import java.util.Optional;

public final class GdsEdition {

    private static final GdsEdition INSTANCE = new GdsEdition();

    public static GdsEdition instance() {
        return INSTANCE;
    }

    void requireEnterpriseEdition(String detail) {
        if (currentState != State.ENTERPRISE) {
            throw new RuntimeException(
                "The requested operation (" + detail +
                ") is only available with the Neo4j Graph Data Science library Enterprise Edition. " +
                "Please refer to the documentation.");
        }
    }

    private enum State {
        ENTERPRISE,
        COMMUNITY,
        INVALID_LICENSE
    }

    private State currentState;

    private Optional<String> errorMessage;

    private GdsEdition() {
        this.currentState = State.COMMUNITY;
    }

    public boolean isOnEnterpriseEdition() {
        return get() == State.ENTERPRISE;
    }

    public boolean isOnCommunityEdition() {
        return get() == State.COMMUNITY;
    }

    public boolean isInvalidLicense() {
        return get() == State.INVALID_LICENSE;
    }

    public Optional<String> errorMessage() {
        return errorMessage;
    }

    public void setToEnterpriseEdition() {
        set(State.ENTERPRISE);
        this.errorMessage = Optional.empty();
    }

    public void setToCommunityEdition() {
        set(State.COMMUNITY);
        this.errorMessage = Optional.empty();
    }

    public void setToInvalidLicense(String errorMessage) {
        set(State.INVALID_LICENSE);
        this.errorMessage = Optional.of(errorMessage);
    }

    private void set(State state) {
        this.currentState = state;
    }

    private State get() {
        return currentState;
    }
}
