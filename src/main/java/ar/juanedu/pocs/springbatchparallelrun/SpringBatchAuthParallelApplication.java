package ar.juanedu.pocs.springbatchparallelrun;

import ar.juanedu.pocs.util.Foo;
import ar.juanedu.pocs.util.JmsSelectorItemReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.jms.JmsItemWriter;
import org.springframework.batch.item.jms.builder.JmsItemWriterBuilder;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.item.support.PassThroughItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.support.converter.MappingJackson2MessageConverter;
import org.springframework.jms.support.converter.MessageConverter;
import org.springframework.jms.support.converter.MessageType;
import org.springframework.lang.Nullable;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.jms.ConnectionFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

@EnableBatchProcessing
@SpringBootApplication
@Slf4j
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

	@Bean // Job configuration
	public Job jmsJob() {
		return this.jobBuilderFactory.get("jmsJob")
				.incrementer(new RunIdIncrementer())
				.start(splitFalopInfoByProcessor())
				.next(step2())
				.build();
	}

	@Bean
	public Step splitFalopInfoByProcessor() {

		return this.stepBuilderFactory.get("splitFalopInfoByProcessor")
				.<Foo, Foo>chunk(10)
				.reader(fromAListWithSize(null))
				.processor(doNothing())
				.writer(toAMsgQueueWithSelector(withThisJmsTemplate(), null))
				//.listener(new StepLogListener())
				.build();
	}

	@Bean
	public Step step2() {
		return this.stepBuilderFactory.get("step2")
				.<Foo, Foo>chunk(10)
				.reader(fromAMsgQueueWithSelector(withThisJmsTemplate(), null))
				.processor(justLogDaFackingObject())
				.writer(toDevNull())
				//.listener(new StepLogListener())
				.build();
	}

	@Bean
	@StepScope //para que se cree la lista en cada ejecucion del step
	public ItemReader<Foo> fromAListWithSize (
			@Value("#{jobParameters['inputSize']}") String inputSize) throws IllegalArgumentException {

		if (inputSize == null) {
			throw new IllegalArgumentException();
		}

		int thisLength = Integer.parseInt(inputSize);
		List<Foo> items = new ArrayList<>(thisLength);
		for (int i = 0; i < thisLength; i++) {
			items.add(new Foo(
					UUID.randomUUID().toString(),
					i,
					giveMeARoutingProcessor(),
					inputSize)
			);
		}
		return new ListItemReader<>(items);
	}

	public ItemProcessor<Foo, Foo> doNothing()
	{
		return new PassThroughItemProcessor<>();
	}

	public ItemProcessor<Foo, Foo> justLogDaFackingObject()
	{
		return item -> {
			log.info(item.toString());
			return item;
		};
	}

	public ItemWriter<Foo> toDevNull()
	{
		return items -> {};
	}

	@Bean
	@StepScope
	public FlatFileItemWriter<Foo> toACsvFileNamed(
			@Value("#{jobParameters['inputSize']}.csv") FileSystemResource filename) {
			return new FlatFileItemWriterBuilder<Foo>()
					.name("customerItemWriter")
					.resource(filename)
					.delimited()
					.names(new String[] {"s",
							"counter",
							"processor",
							"totalSize"})
					.build();
	}

	@Bean
	@StepScope
	public JmsSelectorItemReader<Foo> fromAMsgQueueWithSelector(
			JmsTemplate jmsTemplate,
			@Value("#{stepExecution}") StepExecution stepExecution) {

		long selector = stepExecution.getJobExecution().getJobInstance().getInstanceId();

		JmsSelectorItemReader<Foo> jmsSelectorItemReader = new JmsSelectorItemReader<>();

		jmsSelectorItemReader.setJmsTemplate(jmsTemplate);
		jmsSelectorItemReader.setItemType(Foo.class);
		jmsSelectorItemReader.setSelector("jobInstance = " + Long.valueOf(selector).toString());

		return jmsSelectorItemReader;
	}

	public JmsItemWriter<Foo> toAMsgQueue(JmsTemplate jmsTemplate) {
		return new JmsItemWriterBuilder<Foo>()
				.jmsTemplate(jmsTemplate)
				.build();
	}

	@Bean
	@StepScope
	public ItemWriter<Foo> toAMsgQueueWithSelector(
			JmsTemplate jmsTemplate,
			@Value("#{stepExecution}") StepExecution stepExecution) {
		long selector = stepExecution.getJobExecution().getJobInstance().getInstanceId();

		return list -> list.forEach(item -> jmsTemplate.convertAndSend(item, messagePostProcessor -> {
			messagePostProcessor.setLongProperty("jobInstance", selector);
			return messagePostProcessor;
		}));
	}

	// Spring Batch shit
	@Bean
	public JobLauncher myJobLauncher() throws Exception {
		SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
		jobLauncher.setTaskExecutor(taskExecutor());
		jobLauncher.setJobRepository(jobRepository);
		jobLauncher.afterPropertiesSet();
		return jobLauncher;
	}

	@Bean
	public TaskExecutor taskExecutor() {
		ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
		taskExecutor.setCorePoolSize(10);
		taskExecutor.setMaxPoolSize(20);
		taskExecutor.setQueueCapacity(30);
		return taskExecutor;
	}

	// JMS shit
	@Bean
	public JmsTemplate withThisJmsTemplate() {
		JmsTemplate jmsTemplate = new JmsTemplate(jmsConnectionFactory());
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

	// Serialize message content to json using TextMessage
	public MessageConverter jacksonJmsMessageConverter() {
		MappingJackson2MessageConverter converter = new MappingJackson2MessageConverter();
		converter.setTargetType(MessageType.TEXT);
		converter.setTypeIdPropertyName("_type");
		return converter;
	}

	// Other shit
	private String giveMeARoutingProcessor() {
		return  (Math.random() * 3 + 1) > 3 ? "N" : "V";
	}

	public static void main(String[] args) {
		SpringApplication application = new SpringApplication(SpringBatchAuthParallelApplication.class);
		//application.run(args);

		Properties properties = new Properties();
		// evita que se lance el job automaticamente al arrancar.
		properties.put("spring.batch.job.enabled", false);
		application.setDefaultProperties(properties);

		ConfigurableApplicationContext ctx = application.run(args);

		JobExplorer jex = ctx.getBean("jobExplorer", JobExplorer.class);
		JobLauncher jobLauncher = ctx.getBean("myJobLauncher", JobLauncher.class);
		Job job = ctx.getBean("jmsJob", Job.class);
		//JobParameters jobParameters = new JobParametersBuilder().toJobParameters();

		try {
			JobExecution jobExecution1 = jobLauncher.run(job, new JobParametersBuilder(jex)
					.getNextJobParameters(job).addString("inputSize", "100").toJobParameters());
			JobExecution jobExecution2 = jobLauncher.run(job, new JobParametersBuilder(jex)
					.getNextJobParameters(job).addString("inputSize", "15").toJobParameters());
			JobExecution jobExecution3 = jobLauncher.run(job, new JobParametersBuilder(jex)
					.getNextJobParameters(job).addString("inputSize", "30").toJobParameters());
		} catch (Exception e) {
			e.printStackTrace();
		}

		//SpringApplication.exit(ctx, () -> 0);
	}


}
