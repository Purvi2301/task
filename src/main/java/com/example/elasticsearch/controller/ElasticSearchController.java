package com.example.elasticsearch.controller;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.example.elasticsearch.dto.EmployeeDto;
import com.example.elasticsearch.entity.Employee;
import com.example.elasticsearch.mapper.EmployeeDtoMapper;
import com.example.elasticsearch.service.ElasticSearchService;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/elasticsearch/employees")
public class ElasticSearchController {

    private final ElasticSearchService elasticSearchService;

    public ElasticSearchController(ElasticSearchService elasticSearchService) {
        this.elasticSearchService = elasticSearchService;
    }

    @PostMapping()
    public ResponseEntity<Void> addEmployee(@RequestBody EmployeeDto newEmployee) {
        Employee employee = EmployeeDtoMapper.dtoToEmployee(newEmployee);
        elasticSearchService.addEmployee(employee);
        return new ResponseEntity<>(HttpStatus.CREATED);
    }

    @GetMapping()
    public ResponseEntity<List<EmployeeDto>> findAllEmployees() {
        List<EmployeeDto> employeeDtos = elasticSearchService.findAllEmployees();
        return new ResponseEntity<>(employeeDtos, HttpStatus.OK);
    }

    @GetMapping("/{id}")
    public ResponseEntity<EmployeeDto> findEmployee(@PathVariable String id) {
        EmployeeDto employeeDto = elasticSearchService.findEmployee(id);
        return new ResponseEntity<>(employeeDto, HttpStatus.OK);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteEmployee(@PathVariable String id) {
        elasticSearchService.deleteEmployee(id);
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @GetMapping("/employeesByDept")
    public ResponseEntity<Long> findEmployeesByDept(@RequestParam String department) {
        Long val = elasticSearchService.findEmployeesByDept(department);
        return new ResponseEntity<>(val, HttpStatus.OK);
    }

    @GetMapping("/employeesByZip")
    public ResponseEntity<List<EmployeeDto>> findEmployeesByZip(@RequestParam String zip) {
        List<EmployeeDto> employeeDtos = elasticSearchService.findEmployeesByZip(zip);
        return new ResponseEntity<>(employeeDtos, HttpStatus.OK);
    }

    @GetMapping("/employeesSalGreaterThan9000")
    public ResponseEntity<List<EmployeeDto>> employeesSalGreaterThan9000() {
        List<EmployeeDto> employeeDtos = elasticSearchService.employeesSalGreaterThan9000();
        return new ResponseEntity<>(employeeDtos, HttpStatus.OK);
    }

    //It has some problem with data - giving empty in kibana also
    @GetMapping("/employeesByDepartment")
    public List<Employee> getEmployeesCountByCategory(@RequestParam(defaultValue = "2") int size) {
        SearchHits<Employee> searchHits = elasticSearchService.getEmployeesCountByCategory(Pageable.ofSize(size))
                .getSearchHits();

        return searchHits.getSearchHits()
                .stream()
                .map(SearchHit::getContent)
                .collect(Collectors.toList());
    }

    //It has some problem with data - giving empty in kibana also
    @GetMapping("/getDepartmentsWithEmployeesCount")
    public Map<String, Long> getDepartmentsWithEmployeesCount() throws IOException {
        return elasticSearchService.getDepartmentsWithEmployeesCount();
    }
    @GetMapping("/getDepartmentsWithEmployees")
    public Map<String, List<Map<String, Object>>> getDepartmentsWithEmployees() throws IOException {
        return elasticSearchService.getDepartmentsWithEmployees();
    }

    @GetMapping("/getTotalSalary")
    public double getTotalSalary() throws IOException {
        return elasticSearchService.getTotalSalary();
    }

    @GetMapping("/getMaxSalaryPerDepartment")
    public Map<String, Double> getMaxSalaryPerDepartment() throws IOException {
        return elasticSearchService.getMaxSalaryPerDepartment();
    }

    @GetMapping("/getZipWithEmployees")
    public Map<String, Long> getZipWithEmployees() throws IOException {
        return elasticSearchService.getZipWithEmployees();
    }

    @GetMapping("/getEmployeeByName")
    public ResponseEntity<List<EmployeeDto>> getEmployeeByName() throws IOException {
        List<EmployeeDto> employeeDtos = elasticSearchService.getEmployeeByName();
        return new ResponseEntity<>(employeeDtos, HttpStatus.OK);
    }
}
