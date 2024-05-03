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
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class DepositPropertiesDAOFindSelectionTest extends AbstractDatabaseTest {

    @Test
    public void should_return_records_for_specified_users() {
        var testUser = "testUser";

        // Create a DepositProperties object and persist it
        var dp = new DepositProperties("testId", testUser, "testBag", "testState",
            "testDescription", OffsetDateTime.now(), "testLocation", 1000L, OffsetDateTime.now());
        daoTestExtension.inTransaction(() -> dao.create(dp));
        // TODO add more and prove that other users are not selected

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

        // Assert generated query.
        var sqlMessages = sqlLogger.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
        assertThat(sqlMessages.get(0)) // the order of the predicates is not guaranteed
            .endsWith("where depositpro0_.depositor=?");
        var valueMessages = valuesLogger.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
        assertThat(valueMessages).isEqualTo(List.of(
            "binding parameter [1] as [VARCHAR] - [testUser]"
        ));

        // Assert that the result contains the expected DepositProperties
        assertEquals(1, results.size());
        assertEquals("testId", results.get(0).getDepositId());
        assertEquals(testUser, results.get(0).getDepositor());
    }
}