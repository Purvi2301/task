package com.example.elasticsearch.mapper;

import com.example.elasticsearch.dto.EmployeeDto;
import com.example.elasticsearch.entity.Employee;

import java.util.ArrayList;
import java.util.List;

public class EmployeeDtoMapper {
    public static EmployeeDto employeeToDto(Employee employee) {
        EmployeeDto employeeDto = new EmployeeDto();
        employeeDto.setCity(employee.getCity());
        employeeDto.setDepartment(employee.getDepartment());
        employeeDto.setState(employee.getState());
        employeeDto.setStreet(employee.getStreet());
        employeeDto.setZip(employee.getZip());
        employeeDto.setLastName(employee.getLastName());
        employeeDto.setSalary(employee.getSalary());
        employeeDto.setFirstName(employee.getFirstName());
        employeeDto.setId(employee.getId());
        return employeeDto;
    }

    public static Employee dtoToEmployee(EmployeeDto employeeDto) {
        Employee employee = new Employee();
        employee.setCity(employeeDto.getCity());
        employee.setDepartment(employeeDto.getDepartment());
        employee.setState(employeeDto.getState());
        employee.setStreet(employeeDto.getStreet());
        employee.setZip(employeeDto.getZip());
        employee.setLastName(employeeDto.getLastName());
        employee.setSalary(employeeDto.getSalary());
        employee.setFirstName(employeeDto.getFirstName());
        employee.setId(employeeDto.getId());
        return employee;
    }

    public static List<EmployeeDto> employeesToDtos(List<Employee> employees) {
        List<EmployeeDto> employeeDtos = new ArrayList<>();
        for (Employee employee : employees) {
            EmployeeDto employeeDto = EmployeeDtoMapper.employeeToDto(employee);
            employeeDtos.add(employeeDto);
        }
        return employeeDtos;
    }

}
