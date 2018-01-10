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
import org.springframework.batch.item.database.ItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.LineTokenizer;
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
    public ItemReader<Student> csvFileItemReader() {
        FlatFileItemReader<Student> csvFileReader = new FlatFileItemReader<>();
        csvFileReader.setResource(new ClassPathResource("students.csv"));
        csvFileReader.setLinesToSkip(1);
        csvFileReader.setStrict(true);

        LineMapper<Student> studentLineMapper = createStudentLineMapper();
        csvFileReader.setLineMapper(studentLineMapper);

        return csvFileReader;
    }

    private LineMapper<Student> createStudentLineMapper() {
        DefaultLineMapper<Student> studentLineMapper = new DefaultLineMapper<>();

        LineTokenizer studentLineTokenizer = createStudentLineTokenizer();
        studentLineMapper.setLineTokenizer(studentLineTokenizer);

        FieldSetMapper<Student> studentInformationMapper = createStudentInformationMapper();
        studentLineMapper.setFieldSetMapper(studentInformationMapper);

        return studentLineMapper;
    }

    private LineTokenizer createStudentLineTokenizer() {
        DelimitedLineTokenizer studentLineTokenizer = new DelimitedLineTokenizer();
        studentLineTokenizer.setDelimiter(",");
        studentLineTokenizer.setNames(new String[]{"firstName", "lastName", "email"});
        return studentLineTokenizer;
    }

    private FieldSetMapper<Student> createStudentInformationMapper() {
        BeanWrapperFieldSetMapper<Student> studentInformationMapper = new BeanWrapperFieldSetMapper<>();
        studentInformationMapper.setTargetType(Student.class);
        return studentInformationMapper;
    }

    @Bean
    public ItemWriter<Student> csvFileDatabaseItemWriter(DataSource dataSource, NamedParameterJdbcTemplate jdbcTemplate) {
        JdbcBatchItemWriter<Student> databaseItemWriter = new JdbcBatchItemWriter<>();
        databaseItemWriter.setDataSource(dataSource);
        databaseItemWriter.setJdbcTemplate(jdbcTemplate);

        databaseItemWriter.setSql(QUERY_INSERT_STUDENT);

        ItemSqlParameterSourceProvider<Student> sqlParameterSourceProvider = studentSqlParameterSourceProvider();
        databaseItemWriter.setItemSqlParameterSourceProvider(sqlParameterSourceProvider);

        return databaseItemWriter;
    }

    private ItemSqlParameterSourceProvider<Student> studentSqlParameterSourceProvider() {
        return new BeanPropertyItemSqlParameterSourceProvider<>();
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
