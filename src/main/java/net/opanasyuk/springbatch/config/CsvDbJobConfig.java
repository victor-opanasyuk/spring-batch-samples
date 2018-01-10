package net.opanasyuk.springbatch.config;

import net.opanasyuk.springbatch.model.Student;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;

@Configuration
public class CsvDbJobConfig {

    private static final String QUERY_INSERT_STUDENT = "INSERT " +
            "INTO students(firstName , lastName, email) " +
            "VALUES (:firstName, :lastName, :email)";

    @Bean
    public ItemReader<Student> csvFileItemReader() throws Exception {
        return new FlatFileItemReaderBuilder<Student>()
                .name("csvFileReader")
                .resource(new ClassPathResource("students.csv"))
                .linesToSkip(1)
                .delimited()
                .names(new String[]{"firstName", "lastName", "email"})
                .targetType(Student.class)
                .build();
    }

    @Bean
    public ItemWriter<Student> csvFileDatabaseItemWriter(DataSource dataSource, NamedParameterJdbcTemplate jdbcTemplate) {
        return new JdbcBatchItemWriterBuilder<Student>()
                .dataSource(dataSource)
                .namedParametersJdbcTemplate(jdbcTemplate)
                .sql(QUERY_INSERT_STUDENT)
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .build();
    }

    @Bean
    public Step csvFileToDatabaseStep(ItemReader<Student> csvFileItemReader,
                               ItemProcessor<Student, Student> csvFileItemProcessor,
                               ItemWriter<Student> csvFileDatabaseItemWriter,
                               StepBuilderFactory stepBuilderFactory) {
        return stepBuilderFactory.get("csvFileToDatabaseStep").<Student, Student>chunk(1)
                .reader(csvFileItemReader)
                .processor(csvFileItemProcessor)
                .writer(csvFileDatabaseItemWriter)
                .build();
    }

    @Bean
    public Job csvFileToDatabaseJob(JobBuilderFactory jobBuilderFactory,
                             @Qualifier("csvFileToDatabaseStep") Step csvStudentStep) {
        return jobBuilderFactory.get("csvFileToDatabaseJob")
                .incrementer(new RunIdIncrementer())
                .flow(csvStudentStep)
                .end()
                .build();
    }
}
