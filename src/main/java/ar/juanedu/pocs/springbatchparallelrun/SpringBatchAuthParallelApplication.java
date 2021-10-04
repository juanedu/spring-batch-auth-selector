package ar.juanedu.pocs.springbatchparallelrun;

import ar.juanedu.pocs.util.Foo;
import ar.juanedu.pocs.util.StepLogListener;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.jms.JmsItemReader;
import org.springframework.batch.item.jms.JmsItemWriter;
import org.springframework.batch.item.jms.builder.JmsItemReaderBuilder;
import org.springframework.batch.item.jms.builder.JmsItemWriterBuilder;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.item.support.PassThroughItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.support.converter.MappingJackson2MessageConverter;
import org.springframework.jms.support.converter.MessageConverter;
import org.springframework.jms.support.converter.MessageType;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.jms.ConnectionFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

@EnableBatchProcessing
@EnableBatchIntegration
@SpringBootApplication
public class SpringBatchAuthParallelApplication {

	@Value("${spring.activemq.broker-url}")
	private String brokerUrl;

	@Value("${spring.activemq.queue}")
	private String queue;

	@Value("${spring.activemq.user}")
	private String username;

	@Value("${spring.activemq.password}")
	private String password;

	@Autowired
	private JobRepository jobRepository;

	@Autowired
	private JobExplorer jobExplorer;

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Bean
	public Step step1() throws Exception {
		return this.stepBuilderFactory.get("step1")
				.<Foo, Foo>chunk(10)
				.reader(itemReader(null))
				.processor(itemProcessor())
				.writer(jmsItemWriter(giveMeTheJmsTemplate()))
				.listener(new StepLogListener())
				.build();
	}
	@Bean
	public Step step2() throws Exception {
		return this.stepBuilderFactory.get("step2")
				.<Foo, Foo>chunk(10)
				.reader(jmsItemReader(giveMeTheJmsTemplate()))
				.processor(itemProcessor())
				.writer(itemWriter())
				.listener(new StepLogListener())
				.build();
	}
	@Bean
	public Job jmsJob() throws Exception {
		return this.jobBuilderFactory.get("jmsJob")
				.incrementer(new RunIdIncrementer())
				.start(step1())
				//.next(step2())
				.build();
	}

	@Bean
	public JobLauncher myJobLauncher() throws Exception {
		SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
		jobLauncher.setTaskExecutor(taskExecutor());
		jobLauncher.setJobRepository(jobRepository);
		jobLauncher.afterPropertiesSet();
		return jobLauncher;
	}

	@Bean
	public JobOperator jobOperator(JobRegistry jobRegistry) throws Exception {
		SimpleJobOperator jobOperator = new SimpleJobOperator();
		jobOperator.setJobExplorer(jobExplorer);
		jobOperator.setJobLauncher(myJobLauncher());
		jobOperator.setJobRegistry(jobRegistry);
		jobOperator.setJobRepository(jobRepository);
		return jobOperator;
	}

	@Bean
	public TaskExecutor taskExecutor() {
		ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
		taskExecutor.setCorePoolSize(10);
		taskExecutor.setMaxPoolSize(20);
		taskExecutor.setQueueCapacity(30);
		return taskExecutor;
	}

	@Bean
	@StepScope //para que se cree la lista en cada ejecucion del step
	public ListItemReader<Foo> itemReader(
			@Value("#{jobParameters['inputSize']}") String inputSize) {

		int thisLength = Integer.parseInt(inputSize);
		List<Foo> items = new ArrayList<>(thisLength);
		for (int i = 0; i < thisLength; i++) {
			items.add(new Foo(
					UUID.randomUUID().toString(),
					i,
					giveMeTheProcessor())
			);
		}
		return new ListItemReader<>(items);
	}

	public ItemProcessor<Foo, Foo> itemProcessor()
	{
		return new PassThroughItemProcessor<>();
	}

	@Bean
	public ItemWriter<Foo> itemWriter() {
		return items -> {
			//items.stream().map(item -> ">> current item = " + item).forEach(System.out::println);
		};
	}

	@Bean
	public JmsItemReader<Foo> jmsItemReader(JmsTemplate jmsTemplate) {
		return new JmsItemReaderBuilder<Foo>()
				.jmsTemplate(jmsTemplate)
				.itemType(Foo.class)
				.build();
	}
	@Bean
	public JmsItemWriter<Foo> jmsItemWriter(JmsTemplate jmsTemplate) {
		return new JmsItemWriterBuilder<Foo>()
				.jmsTemplate(jmsTemplate)
				.build();
	}

	// JMS shit
	@Bean
	public JmsTemplate giveMeTheJmsTemplate() {
		JmsTemplate jmsTemplate = new JmsTemplate();
		jmsTemplate.setConnectionFactory(jmsConnectionFactory());
		jmsTemplate.setDefaultDestinationName(this.queue);
		jmsTemplate.setMessageConverter(jacksonJmsMessageConverter());
		jmsTemplate.setReceiveTimeout(500);
		return jmsTemplate;
	}

	@Bean
	public ConnectionFactory jmsConnectionFactory() {
		ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(this.brokerUrl);
		connectionFactory.setPassword(this.password);
		connectionFactory.setUserName(this.username);
		connectionFactory.setUseAsyncSend(true);
		connectionFactory.getPrefetchPolicy().setQueuePrefetch(1);
		return connectionFactory;
	}

	@Bean // Serialize message content to json using TextMessage
	public MessageConverter jacksonJmsMessageConverter() {
		MappingJackson2MessageConverter converter = new MappingJackson2MessageConverter();
		converter.setTargetType(MessageType.TEXT);
		converter.setTypeIdPropertyName("_type");
		return converter;
	}

	// Other shit
	private String giveMeTheProcessor() {
		return  (Math.random() * 3 + 1) > 3 ? "N" : "V";
	}

	public static void main(String[] args) {
		SpringApplication application = new SpringApplication(SpringBatchAuthParallelApplication.class);
		//application.run(args);

		Properties properties = new Properties();
		properties.put("spring.batch.job.enabled", false);
		application.setDefaultProperties(properties);

		ConfigurableApplicationContext ctx = application.run(args);

		JobExplorer jex = ctx.getBean("jobExplorer", JobExplorer.class);
		JobLauncher jobLauncher = ctx.getBean("myJobLauncher", JobLauncher.class);
		Job job = ctx.getBean("jmsJob", Job.class);
		//JobParameters jobParameters = new JobParametersBuilder().toJobParameters();

		try {
			JobExecution jobExecution1 = jobLauncher.run(job, new JobParametersBuilder(jex)
					.getNextJobParameters(job).addString("inputSize", "500").toJobParameters());
			JobExecution jobExecution2 = jobLauncher.run(job, new JobParametersBuilder(jex)
					.getNextJobParameters(job).addString("inputSize", "750").toJobParameters());
		} catch (Exception e) {
			e.printStackTrace();
		}

		//SpringApplication.exit(ctx, () -> 0);
	}


}
