package com.example.elasticsearch.service;

import co.elastic.clients.elasticsearch._types.aggregations.*;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.JsonData;
import com.example.elasticsearch.dto.EmployeeDto;
import com.example.elasticsearch.entity.Employee;
import com.example.elasticsearch.mapper.EmployeeDtoMapper;
import com.example.elasticsearch.repository.ElasticSearchRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.client.elc.NativeQuery;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHitSupport;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.SearchPage;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.Criteria;
import org.springframework.data.elasticsearch.core.query.CriteriaQuery;
import org.springframework.data.elasticsearch.core.query.Query;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import co.elastic.clients.elasticsearch.ElasticsearchClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class ElasticSearchService {

    private static final String DOCUMENT_INDEX = "employeeindex";
    @Autowired
    private ElasticSearchRepository repository;
    private final ElasticsearchOperations elasticsearchOperations;
    private final ElasticsearchClient elasticsearchClient;

    public ElasticSearchService(ElasticsearchOperations elasticsearchOperations, ElasticsearchClient elasticsearchClient) {
        this.elasticsearchOperations = elasticsearchOperations;
        this.elasticsearchClient = elasticsearchClient;
    }


    public void addEmployee(@RequestBody Employee newEmployee) {
        if (!repository.existsById(newEmployee.getId())) {
            repository.save(newEmployee);
        }
    }

    public List<EmployeeDto> findAllEmployees() {
        List<Employee> employees = new ArrayList<>();
        repository.findAll().forEach(employees::add);
        return EmployeeDtoMapper.employeesToDtos(employees);
    }

    public EmployeeDto findEmployee(@PathVariable String id) {
        Employee employee = repository.findById(id).orElse(null);
        assert employee != null;
        return EmployeeDtoMapper.employeeToDto(employee);
    }

    public void deleteEmployee(@PathVariable String id) {
        repository.deleteById(id);
    }

    public Long findEmployeesByDept(@RequestParam String department) {
        return repository.findEmployeesByDepartment(department, PageRequest.of(0, 10));
    }

    public List<EmployeeDto> findEmployeesByZip(@RequestParam String zip) {
        CriteriaQuery query = new CriteriaQuery(new Criteria("zip").is(zip));

        SearchHits<Employee> searchHits = elasticsearchOperations.search(query, Employee.class, IndexCoordinates.of(DOCUMENT_INDEX));

        List<Employee> employees = new ArrayList<>();
        searchHits.forEach(hit -> employees.add(hit.getContent()));

        return EmployeeDtoMapper.employeesToDtos(employees);
    }

    public List<EmployeeDto> employeesSalGreaterThan9000() {
        CriteriaQuery query = new CriteriaQuery(new Criteria("salary").greaterThan(9000));

        SearchHits<Employee> searchHits = elasticsearchOperations.search(query, Employee.class, IndexCoordinates.of(DOCUMENT_INDEX));

        List<Employee> employees = new ArrayList<>();
        searchHits.forEach(hit -> employees.add(hit.getContent()));

        return EmployeeDtoMapper.employeesToDtos(employees);
    }

    public Map<String, Long> getDepartmentsWithEmployeesCount() throws IOException {
        SearchRequest request = new SearchRequest.Builder()
                .index("employeeindex")
                .aggregations("department_aggregation",
                        Aggregation.of(a -> a.terms(TermsAggregation.of(t -> t.field("department")))))
                .build();

        SearchResponse<Void> response = elasticsearchClient.search(request, Void.class);

        return response.aggregations().get("department_aggregation")
                .sterms().buckets().array()
                .stream()
                .collect(Collectors.toMap(
                        b -> b.key().stringValue(),
                        MultiBucketBase::docCount
                ));
    }

    public double getTotalSalary() throws IOException {
        SearchRequest request = new SearchRequest.Builder()
                .index("employeeindex")
                .aggregations("total",
                        Aggregation.of(a -> a.sum(SumAggregation.of(t -> t.field("salary")))))
                .build();
        SearchResponse<Void> response = elasticsearchClient.search(request, Void.class);

        return response.aggregations().get("total").sum().value();
    }

    public SearchPage<Employee> getEmployeesCountByCategory(Pageable pageable) {
        Query query = NativeQuery.builder()
                .withAggregation("department_aggregation",
                        Aggregation.of(b -> b.terms(t -> t.field("department"))))
                .build();

        SearchHits<Employee> response = elasticsearchOperations.search(query, Employee.class);
        return SearchHitSupport.searchPageFor(response, pageable);
    }

    public Map<String, Double> getMaxSalaryPerDepartment() throws IOException {
        SearchRequest request = new SearchRequest.Builder()
                .index("employeeindex")
                .aggregations("departments",
                        Aggregation.of(a -> a.terms(
                                TermsAggregation.of(t -> t.field("department"))
                        ).aggregations("max_salary",
                                Aggregation.of(m -> m.max(MaxAggregation.of(s -> s.field("salary"))))
                        )))
                .build();

        SearchResponse<Void> response = elasticsearchClient.search(request, Void.class);

        Map<String, Double> maxSalaries = new HashMap<>();
        response.aggregations().get("departments").sterms().buckets().array()
                .forEach(bucket -> {
                    String department = bucket.key().stringValue();
                    double maxSalary = bucket.aggregations().get("max_salary").max().value();
                    maxSalaries.put(department, maxSalary);
                });

        return maxSalaries;
    }

    public Map<String, Long> getZipWithEmployees() throws IOException {
        SearchRequest request = new SearchRequest.Builder()
                .index("employeeindex")
                .aggregations("zip_aggregation",
                        Aggregation.of(a -> a.terms(TermsAggregation.of(t -> t.field("zip")))))
                .build();

        SearchResponse<Void> response = elasticsearchClient.search(request, Void.class);

        return response.aggregations().get("zip_aggregation")
                .sterms().buckets().array()
                .stream()
                .collect(Collectors.toMap(
                        b -> b.key().stringValue(),
                        MultiBucketBase::docCount
                ));
    }

    public Map<String, List<Map<String, Object>>> getDepartmentsWithEmployees() throws IOException {
        SearchRequest request = new SearchRequest.Builder()
                .index("employeeindex")
                .query(q -> q.bool(b -> b.must(m -> m.term(t -> t.field("salary").value(30000)))))
                .aggregations("department_aggregation",
                        Aggregation.of(a -> a.terms(TermsAggregation.of(t -> t.field("department")))))
                .query(q -> q.term(t -> t.field("firstName").value("Purvi")))
                .build();

        SearchResponse<JsonData> response = elasticsearchClient.search(request, JsonData.class);

        List<Hit<JsonData>> hits = response.hits().hits();
        Map<String, List<Map<String, Object>>> departmentEmployees = new HashMap<>();

        for (Hit<JsonData> hit : hits) {  
            Map<String, Object> employeeData = hit.source().to(Map.class);
            String department = (String) employeeData.get("department");

            departmentEmployees
                    .computeIfAbsent(department, k -> new ArrayList<>())
                    .add(employeeData);
        }

        return departmentEmployees;
    }

    public List<EmployeeDto> getEmployeeByName() throws IOException {
        SearchRequest request = new SearchRequest.Builder()
                .index("employeeindex")
                .query(q -> q.match(m -> m.field("firstName").query("Purvi")))
                .build();
        SearchResponse<JsonData> response = elasticsearchClient.search(request, JsonData.class);
        List<Hit<JsonData>> hits = response.hits().hits();
        List<EmployeeDto> employeeData = new ArrayList<>();
        for (Hit<JsonData> hit : hits) {
            employeeData = hit.source().to(List.class);
        }
        return employeeData;
    }
}
