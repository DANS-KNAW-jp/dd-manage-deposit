/*
 * Copyright (C) 2023 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.managedeposit.db;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import nl.knaw.dans.managedeposit.AbstractDatabaseTest;
import nl.knaw.dans.managedeposit.core.DepositProperties;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;

import static nl.knaw.dans.managedeposit.TestUtils.captureLog;
import static org.assertj.core.api.Assertions.assertThat;

public class DepositPropertiesDAOFindSelectionTest extends AbstractDatabaseTest {

    @Test
    public void should_return_records_for_specified_users() {
        var testUser = "User2";

        // Create a list of DepositProperties objects and persist them
        var now = OffsetDateTime.now();
        var dps = List.of(
            new DepositProperties("Id1", "User1", "Bag1", "State1",
                "Description1", now.plusHours(1), "Location1", 1000L, now.plusHours(2)),
            new DepositProperties("Id2", "User1", "Bag2", "State2",
                "Description2", now.plusMinutes(3), "Location2", 2000L, now.plusMinutes(4)),
            new DepositProperties("Id3", testUser, "Bag3", "State3",
                "Description3", now.plusSeconds(5), "Location3", 3000L, now.plusSeconds(6)),
            new DepositProperties("Id4", testUser, "Bag4", "State4",
                "Description4", now.plusNanos(7), "Location4", 4000L, now.plusNanos(8)),
            new DepositProperties("Id5", "User3", "Bag5", "State5",
                "Description5", now.plusHours(9), "Location5", 5000L, now.plusHours(10)),
            new DepositProperties("Id6", "User3", "Bag6", "State6",
                "Description6", now.plusMinutes(11), "Location6", 6000L, now.plusMinutes(12))
        );
        daoTestExtension.inTransaction(() -> dps.forEach(dp -> dao.create(dp)));

        var sqlLogger = captureLog(Level.DEBUG, "org.hibernate.SQL");
        var valuesLogger = captureLog(Level.TRACE, "org.hibernate.type.descriptor.sql.BasicBinder");

        // Create query parameters
        var queryParameters = Map.of(
            "user", List.of(testUser)
        );

        var results = daoTestExtension.inTransaction(() ->
            // method under test
            dao.findSelection(queryParameters)
        );

        // Assert generated where clause and bound values
        var sqlMessages = sqlLogger.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
        assertThat(sqlMessages.get(0)) // the order of the predicates is not guaranteed
            .endsWith("where depositpro0_.depositor=?");
        var valueMessages = valuesLogger.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
        assertThat(valueMessages).isEqualTo(List.of(
            "binding parameter [1] as [VARCHAR] - [%s]".formatted(testUser)
        ));

        // Assert that the result contains the expected DepositProperties
        assertThat(results)
            .hasSize(2)
            .extracting(DepositProperties::getDepositor)
            .containsOnly(testUser);
        assertThat(results)
            .extracting(DepositProperties::getDepositId)
            .containsExactlyInAnyOrder("Id3", "Id4");
    }
}