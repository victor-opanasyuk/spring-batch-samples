package net.opanasyuk.springbatch.common;

import net.opanasyuk.springbatch.model.Student;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Component
public class StudentProcessor implements ItemProcessor<Student, Student> {

    private static final Logger LOGGER = LoggerFactory.getLogger(StudentProcessor.class);

    @Override
    public Student process(Student item) throws Exception {
        LOGGER.info("Processing student information: {}", item);
        return item;
    }
}
