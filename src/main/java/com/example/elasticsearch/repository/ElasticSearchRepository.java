package com.example.elasticsearch.repository;

import java.util.Optional;

import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.annotations.CountQuery;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;
import com.example.elasticsearch.entity.Employee;

@Repository
public interface ElasticSearchRepository extends ElasticsearchRepository<Employee, String> {

    Optional<Employee> findById(String id);

    @CountQuery("{\"bool\": {\"must\": [{\"match\": {\"department\": \"?0\"}}]}}")
    Long findEmployeesByDepartment(String department, Pageable pageable);

}