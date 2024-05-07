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

import io.dropwizard.hibernate.AbstractDAO;
import nl.knaw.dans.managedeposit.core.DepositProperties;
import org.hibernate.SessionFactory;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaDelete;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.CriteriaUpdate;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.lang.Boolean.parseBoolean;
import static java.lang.String.join;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;

@SuppressWarnings("resource")
public class DepositPropertiesDAO extends AbstractDAO<DepositProperties> {
    private static final Logger log = LoggerFactory.getLogger(DepositPropertiesDAO.class);

    public DepositPropertiesDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public Optional<DepositProperties> findById(String depositId) {
        return Optional.ofNullable(get(depositId));
    }

    public DepositProperties create(DepositProperties dp) {
        return persist(dp);
    }

    public DepositProperties save(DepositProperties dp) {
        return persist(dp);
    }

    public void merge(DepositProperties dp) {
        currentSession().merge(dp);
    }

    public void delete(DepositProperties dp) {
        currentSession().delete(dp);
    }

    public List<DepositProperties> findAll() {
        return currentSession().createQuery("from DepositProperties", DepositProperties.class).list();
    }

    public List<DepositProperties> findSelection(Map<String, List<String>> queryParameters) {
        CriteriaBuilder criteriaBuilder = currentSession().getCriteriaBuilder();

        if (queryParameters.isEmpty())
            return findAll();

        CriteriaQuery<DepositProperties> criteriaQuery = criteriaBuilder.createQuery(DepositProperties.class);
        Root<DepositProperties> root = criteriaQuery.from(DepositProperties.class);
        Predicate predicate = buildQueryCriteria(queryParameters, criteriaBuilder, root);
        criteriaQuery.select(root).where(predicate);
        Query<DepositProperties> query = currentSession().createQuery(criteriaQuery);
        return query.getResultList();
    }

    public Optional<Integer> deleteSelection(Map<String, List<String>> queryParameters) {
        var criteriaBuilder = currentSession().getCriteriaBuilder();
        if (queryParameters.isEmpty())                   // Note: all records will be deleted (accidentally) without any specified query parameter
            return Optional.of(0);

        CriteriaDelete<DepositProperties> deleteQuery = criteriaBuilder.createCriteriaDelete(DepositProperties.class);
        Root<DepositProperties> root = deleteQuery.from(DepositProperties.class);

        Predicate predicate = buildQueryCriteria(queryParameters, criteriaBuilder, root);

        deleteQuery.where(predicate);
        var query = currentSession().createQuery(deleteQuery);
        return Optional.of(query.executeUpdate());
    }

    private Predicate buildQueryCriteria(Map<String, List<String>> queryParameters, CriteriaBuilder criteriaBuilder, Root<DepositProperties> root) throws IllegalArgumentException {
        var lowerCaseQueryParameters = new HashMap<String, List<String>>();
        queryParameters.forEach((key, value) -> lowerCaseQueryParameters.put(key.toLowerCase(), value));

        var paramToField = new LinkedHashMap<String, String>() {{
            put("depositid", "depositId");
            put("user", "depositor");
            put("state", "depositState");
            put("deleted", "deleted");
            put("startdate", "depositCreationTimestamp");
            put("enddate", "depositCreationTimestamp");
        }};
        var illegalKeys = lowerCaseQueryParameters.keySet().stream()
            .filter(key -> !paramToField.containsKey(key))
            .collect(Collectors.toSet());
        if (!illegalKeys.isEmpty()) {
            log.error("The following query parameters are ignored: " + join(", ", illegalKeys));
        }

        var predicates = new ArrayList<Predicate>();
        if (lowerCaseQueryParameters.get("startdate").isEmpty() || lowerCaseQueryParameters.get("enddate").isEmpty()) {
            predicates.add(criteriaBuilder.isNull(root.get("depositCreationTimestamp")));
            // TODO log.error if the other is not empty: nothing will be found
        }
        paramToField.keySet().forEach(paramName -> {
            var fieldName = paramToField.get(paramName);
            var orPredicates = new ArrayList<Predicate>();
            var values = lowerCaseQueryParameters.get(paramName);
            if (values != null && !values.isEmpty()) {
                values.forEach(value -> {
                    switch (paramName) {
                        // TODO (perhaps outside the paramToField loop)
                        //  turn: ... and (dct>start1 or dct>start2) and (dct<end1 or dct<end2)
                        //  into: ... and ((dct between start1 and end1) or (dct between start2 and end2))
                        //  or throw an exception in case of repeated start/end dates
                        case "startdate":
                            orPredicates.add(criteriaBuilder.greaterThan(root.get(fieldName), parseDate(value)));
                            break;
                        case "enddate":
                            orPredicates.add(criteriaBuilder.lessThan(root.get(fieldName), parseDate(value)));
                            break;
                        case "deleted":
                            orPredicates.add(criteriaBuilder.equal(root.get(fieldName), parseBoolean(value)));
                            break;
                        default:
                            orPredicates.add(criteriaBuilder.equal(root.get(fieldName), value));
                            break;
                    }
                });
                predicates.add(criteriaBuilder.or(orPredicates.toArray(new Predicate[0])));
            }
        });
        return criteriaBuilder.and(predicates.toArray(new Predicate[0]));
    }

    private static OffsetDateTime parseDate(String value) {
        return LocalDate.parse(value, ISO_LOCAL_DATE)
            .atStartOfDay()
            .toInstant(ZoneOffset.UTC)
            .atOffset(ZoneOffset.UTC);
    }

    public Optional<Integer> updateDeleteFlag(String depositId, boolean deleted) {
        CriteriaBuilder criteriaBuilder = currentSession().getCriteriaBuilder();
        CriteriaUpdate<DepositProperties> criteriaUpdate = criteriaBuilder.createCriteriaUpdate(DepositProperties.class);
        Root<DepositProperties> root = criteriaUpdate.from(DepositProperties.class);

        Predicate predicate = buildQueryCriteria(Map.of("depositId", List.of(depositId)), criteriaBuilder, root);
        criteriaUpdate.where(predicate);

        criteriaUpdate.set("deleted", deleted);

        var query = currentSession().createQuery(criteriaUpdate);
        return Optional.of(query.executeUpdate());
    }

}
