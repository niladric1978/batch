package com.example.batch;


import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
//import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
//import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.configuration.support.DefaultBatchConfiguration;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
//import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.core.step.builder.PartitionStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.sql.DataSource;
import java.io.IOException;
import java.net.MalformedURLException;
import java.text.ParseException;

@Configuration
@EnableBatchProcessing
//@Profile("spring")
//@EnableTransactionManagement
public class SpringbatchPartitionConfig {

    @Autowired
    ResourcePatternResolver resoursePatternResolver;

    @Value("org/springframework/batch/core/schema-drop-sqlite.sql")
    private Resource dropReopsitoryTables;

    @Value("org/springframework/batch/core/schema-sqlite.sql")
    private Resource dataReopsitorySchema;
  //  @Autowired
  //  DataSource dataSource;

  /*  @Autowired
    JobRepository jobRepository;

    @Autowired
    PlatformTransactionManager platformTransactionManager;
*/
  /*  @Autowired
    public JobBuilderFactory jobs;

    @Autowired
    public StepBuilderFactory steps; */

    @Bean(name = "partitionerJob")
    public Job myJob(JobRepository jobRepository, DataSource dataSource) throws Exception{
        return new JobBuilder("myJob", jobRepository)
                .start(partitionStep(jobRepository,(PlatformTransactionManager) transactionManager(dataSource)))
                .build();
    }

  /*  public Job partitionerJob() throws UnexpectedInputException, MalformedURLException, ParseException {
        return jobs.get("partitionerJob")
                .start(partitionStep())
                .build();
    } */

 /*   @Bean
    public Step partitionStep() throws UnexpectedInputException, MalformedURLException, ParseException {
        return steps.get("partitionStep")
                .partitioner("slaveStep", partitioner())
                .step(slaveStep())
                .taskExecutor(taskExecutor())
                .build();

*/
 public Step partitionStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) throws UnexpectedInputException, MalformedURLException, ParseException{

        return new StepBuilder("step",jobRepository).partitioner("step",partitioner()).gridSize(4)
                .step(slaveStep(jobRepository,transactionManager)).build();
    }

    @Bean
    public Step slaveStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) throws UnexpectedInputException, MalformedURLException, ParseException {
        return new StepBuilder("step",jobRepository).<Transaction, Transaction>chunk(1,
                        transactionManager)
                .reader(itemReader(null))
                .writer(itemWriter(null))
                .build();
    }

    @Bean
    public CustomSimplePartitioner partitioner() {
       // CustomMultiResourcePartitioner partitioner = new CustomMultiResourcePartitioner();
        CustomSimplePartitioner partitioner=new CustomSimplePartitioner();
        Resource[] resources;
        try {
            resources = resoursePatternResolver.getResources("file:src/main/resources/input/partitioner/*.csv");
        } catch (IOException e) {
            throw new RuntimeException("I/O problems when resolving the input file pattern.", e);
        }

       // partitioner.setResources(resources);

        return partitioner;
    }

    @Bean
    @StepScope
    public FlatFileItemReader<Transaction> itemReader(@Value("#{stepExecutionContext[partitionNum]}") Integer pNum) throws UnexpectedInputException, ParseException {
        CustomFileReader<Transaction> reader = new CustomFileReader<>(4,pNum);
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        System.out.println("in reader.."+ pNum);
        String[] tokens = {"username", "userid", "transactiondate", "amount"};
        tokenizer.setNames(tokens);
        reader.setResource(new ClassPathResource("input/partitioner/" + "record1.csv"));
        DefaultLineMapper<Transaction> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(new RecordFieldSetMapper());
        reader.setLinesToSkip(1);
        reader.setLineMapper(lineMapper);
        return reader;
    }


    @Bean
    @StepScope
    public FlatFileItemReader<Transaction> itemReaderNew(@Value("#{stepExecutionContext[fileName]}") String filename) throws UnexpectedInputException, ParseException {
        FlatFileItemReader<Transaction> reader = new FlatFileItemReader<>();
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        String[] tokens = {"username", "userid", "transactiondate", "amount"};
        tokenizer.setNames(tokens);
        reader.setResource(new ClassPathResource("input/partitioner/" + filename));
        DefaultLineMapper<Transaction> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(new RecordFieldSetMapper());
        reader.setLinesToSkip(1);
        reader.setLineMapper(lineMapper);
        return reader;
    }

    @Bean(destroyMethod = "")
    @StepScope
    public ItemWriter<Transaction> itemWriter(@Value("#{stepExecutionContext[partitionNum]}") String partitionNum) throws MalformedURLException {
        ItemWriter<Transaction> itemWriter = new ItemWriter<Transaction>() {
            @Override
            public void write(Chunk<? extends Transaction> chunk) throws Exception {
                System.out.println("chunk="+chunk.getItems() + partitionNum);
            }

        };
        System.out.println("writer...");
        return itemWriter;


       // itemWriter.setMarshaller(marshaller);
       // itemWriter.setRootTagName("transactionRecord");

       // return itemWriter;
    }


   // @Bean(destroyMethod = "")
    @StepScope
    public StaxEventItemWriter<Transaction> itemWriterNew( @Value("#{stepExecutionContext[opFileName]}") String filename) throws MalformedURLException {
        StaxEventItemWriter<Transaction> itemWriter = new StaxEventItemWriter<>();
        // itemWriter.setMarshaller(marshaller);
        // itemWriter.setRootTagName("transactionRecord");
        itemWriter.setResource(new FileSystemResource("src/main/resources/output/" + filename));
        return itemWriter;
    }

 /*   @Bean
    public Marshaller marshaller() {
        Jaxb2Marshaller marshaller = new Jaxb2Marshaller();
        marshaller.setClassesToBeBound(Transaction.class);
        return marshaller;
    } */

    @Bean
    public TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setMaxPoolSize(5);
        taskExecutor.setCorePoolSize(5);
        taskExecutor.setQueueCapacity(5);
        taskExecutor.afterPropertiesSet();
        return taskExecutor;
    }

    //@Bean
    public JobRepository getJobRepository(DataSource dataSource) throws Exception {
        JobRepositoryFactoryBean factory = new JobRepositoryFactoryBean();
       // factory.setDataSource(dataSource());
        factory.setDataSource(dataSource);
        factory.setTransactionManager((PlatformTransactionManager) transactionManager(dataSource));
        // JobRepositoryFactoryBean's methods Throws Generic Exception,
        // it would have been better to have a specific one
        factory.afterPropertiesSet();
        return factory.getObject();
    }


    @Bean
    public DataSource dataSource() {
        EmbeddedDatabaseBuilder builder = new EmbeddedDatabaseBuilder();
        return builder.setType(EmbeddedDatabaseType.HSQL)
                //.addScript("classpath:org/springframework/batch/core/schema-drop-h2.sql")
                //.addScript("classpath:org/springframework/batch/core/schema-h2.sql")
                .addScript("/org/springframework/batch/core/schema-hsqldb.sql")
                .generateUniqueName(true)
                .build();
    }



    @Bean
    public TransactionManager transactionManager(DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }

    //public JobLauncher getJobLauncher() throws Exception {
        //SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
        // SimpleJobLauncher's methods Throws Generic Exception,
        // it would have been better to have a specific one
       // jobLauncher.setJobRepository(getJobRepository());
       // jobLauncher.afterPropertiesSet();
        //return jobLauncher;
   // }
}