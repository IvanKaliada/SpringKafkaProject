package com.iikaliada.spring.kafka.project.controller;

import com.iikaliada.spring.kafka.project.model.EmployeeModel;
import com.iikaliada.spring.kafka.project.producer.EmployeeKafkaProducer;
import com.iikaliada.spring.kafka.project.schema.Employee;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Random;

@RestController
public class EmployeeController {

    @Autowired
    private EmployeeKafkaProducer employeeKafkaProducer;

    @PostMapping(value = "/employee")
    public void sendEmployee(@RequestBody EmployeeModel model) {
        Employee employee = Employee.newBuilder()
                .setFirstName(model.getFirstName())
                .setLastName(model.getLastName())
                .setAge(model.getAge())
                .setId(new Random(1000).nextInt())
                .build();
        employeeKafkaProducer.send(employee);
    }

}
