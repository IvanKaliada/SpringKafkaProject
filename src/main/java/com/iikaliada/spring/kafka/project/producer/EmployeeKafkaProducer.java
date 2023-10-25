package com.iikaliada.spring.kafka.project.producer;

import com.iikaliada.spring.kafka.project.schema.Employee;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Service
@RequiredArgsConstructor
public class EmployeeKafkaProducer {

    private static final Logger log = LoggerFactory.getLogger(EmployeeKafkaProducer.class);

    private final KafkaTemplate<String, Employee> kafkaTemplate;

    @Value("${avro.topic.name}")
    private String employeeTopic;

    public void send(Employee employee) {
        ListenableFuture<SendResult<String, Employee>> listenableFuture = kafkaTemplate.send(employeeTopic, String.valueOf(employee.getId()), employee);
        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<String, Employee>>() {
            @Override
            public void onFailure(Throwable ex) {
                log.error("Message failed to produce", ex);
            }

            @Override
            public void onSuccess(SendResult<String, Employee> result) {
                log.info("Message successfully produced");
            }
        });
    }

}
