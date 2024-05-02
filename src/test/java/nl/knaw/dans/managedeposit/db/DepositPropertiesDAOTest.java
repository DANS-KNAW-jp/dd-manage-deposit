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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DepositPropertiesDAOTest extends AbstractDatabaseTest { // TODO split in class per test

    @Test
    public void testFindSelection() {
        daoTestExtension.inTransaction(() -> { // TODO minimize scope
            // Create a DepositProperties object and persist it
            DepositProperties dp = new DepositProperties("testId", "testUser", "testBag", "testState",
                "testDescription", OffsetDateTime.now(), "testLocation", 1000L, OffsetDateTime.now());
            dao.create(dp);

            // Call findSelection with query parameters
            Map<String, List<String>> queryParameters = Map.of(
                "depositId", List.of("testId"),
                "user", List.of("testUser")
            );
            List<DepositProperties> results = dao.findSelection(queryParameters);

            // Assert that the result contains the expected DepositProperties
            assertEquals(1, results.size());
            assertEquals("testId", results.get(0).getDepositId());
            assertEquals("testUser", results.get(0).getDepositor());
        });
    }

    @Test
    public void testDeleteSelection() {
        daoTestExtension.inTransaction(() -> { // TODO minimize scope
            // Create a DepositProperties object and persist it
            DepositProperties dp = new DepositProperties("testId", "testUser", "testBag", "testState",
                "testDescription", OffsetDateTime.now(), "testLocation", 1000L, OffsetDateTime.now());
            dao.create(dp);

            // Call deleteSelection with query parameters
            Map<String, List<String>> queryParameters = Map.of(
                "depositId", List.of("testId"),
                "user", List.of("testUser")
            );
            int deletedCount = dao.deleteSelection(queryParameters).orElse(0);

            // Assert that the correct number of records were deleted
            assertEquals(1, deletedCount);

            // Assert that the deleted record no longer exists
            List<DepositProperties> results = dao.findSelection(queryParameters);
            assertTrue(results.isEmpty());
        });
    }

    @Test
    public void testUpdateDeleteFlag() {
        String depositId = "testId";
        boolean deleted = true;

        // Create a DepositProperties object and persist it
        DepositProperties dp = new DepositProperties("testId", "testUser", "testBag", "testState",
            "testDescription", OffsetDateTime.now(), "testLocation", 1000L, OffsetDateTime.now());
        daoTestExtension.inTransaction(() ->
            dao.create(dp)
        );

        var sqlLogger = captureLog(Level.DEBUG, "org.hibernate.SQL");
        var valuesLogger = captureLog(Level.TRACE, "org.hibernate.type.descriptor.sql.BasicBinder");

        // method under test
        int updatedCount = daoTestExtension.inTransaction(() ->
            dao.updateDeleteFlag(depositId, deleted).orElse(0)
        );

        // skip create, which is captured despite execution before setting up the loggers
        assertThat(sqlLogger.list.stream().map(ILoggingEvent::getFormattedMessage).toList())
            .endsWith("update deposit_properties set deleted=? where deposit_id=?");
        assertThat(valuesLogger.list.stream().map(ILoggingEvent::getFormattedMessage).toList())
            .endsWith(
                "binding parameter [1] as [BOOLEAN] - [true]",
                "binding parameter [2] as [VARCHAR] - [testId]");

        // Assert that the correct number of records were updated
        assertEquals(1, updatedCount);

        // Assert that the updated record has the correct deleted flag
        DepositProperties updatedDp = daoTestExtension.inTransaction(() ->
            dao.findById(depositId).orElse(null)
        );
        assertNotNull(updatedDp);
        assertEquals(deleted, updatedDp.isDeleted());
    }
}