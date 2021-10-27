package ar.juanedu.pocs.springbatchparallelrun;

import ar.juanedu.pocs.util.*;
import com.google.common.util.concurrent.RateLimiter;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.support.ClassifierCompositeItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.item.support.PassThroughItemProcessor;
import org.springframework.batch.item.support.builder.ClassifierCompositeItemWriterBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.classify.Classifier;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.FileSystemResource;
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
	public Job parallelComplexJob() {

		Flow customTaskFlow = new FlowBuilder<Flow>("customTask").start(processCustomQueue(null)).build();
		Flow generalTaskFlow = new FlowBuilder<Flow>("generalTask").start(processGeneralQueue(null)).build();

		Flow parallelTasksFlow = new FlowBuilder<Flow>("parallelTasksFlow")
				.split(taskExecutor())
				.add(generalTaskFlow, customTaskFlow)
				.build();

		Flow initialTaskFlow = new FlowBuilder<SimpleFlow>("initialTaskFlow")
				.start(splitFalopInfoByProcessor())
				.build();

		return this.jobBuilderFactory.get("parallelComplexJob")
				.start(initialTaskFlow)
				.next(parallelTasksFlow)
				.next(finalDump())
				.build()
				.incrementer(new RunIdIncrementer())
				.build();
	}

	@Bean // Primer Step que crea los registros y los divide en dos colas
	public Step splitFalopInfoByProcessor() {

		return this.stepBuilderFactory.get("splitFalopInfoByProcessor")
				.<Foo, Foo>chunk(10)
				.reader(fromAListWithSize(null))
				.processor(doNothing())
				.writer(splitToDifferentQueues())
				.listener(new StepLogListener())
				.build();
	}

	@Bean // Segundo Step que asigna el valor del thread al campo Foo.auth a 20 TPS y graba a cola comun. Foo.processor = "V"
	public Step processGeneralQueue(
			@Qualifier("withThisJmsTemplateForVisa") JmsTemplate jmsTemplate
	) {
		AuthProcessor processor = new AuthProcessor(RateLimiter.create(20));

		return this.stepBuilderFactory.get("processGeneralQueue")
				.<Foo, Foo>chunk(10)
				.reader(fromVisaQueue(jmsTemplate, null))
				//.processor(authAndLogDaVisaObject(null))
				.processor(processor)
				.writer(joinedQueueWriter(null, null))
				.listener(new StepLogListener())
				.build();
	}

	@Bean // Segundo Step paralelo que procesa trn con Foo.processor "N" a 1 TPS
	public Step processCustomQueue(
			@Qualifier("withThisJmsTemplateForOthers") JmsTemplate jmsTemplate
	) {
		AuthProcessor processor = new AuthProcessor(RateLimiter.create(1));

		return this.stepBuilderFactory.get("processCustomQueue")
				.<Foo, Foo>chunk(10)
				.reader(fromOthersQueue(jmsTemplate, null))
				//.processor(authAndLogDaOthersObject(null))
				.processor(processor)
				.writer(joinedQueueWriter(null, null))
				.listener(new StepLogListener())
				.build();
	}

	@Bean
	public Step finalDump() {
		return this.stepBuilderFactory.get("finalDump")
				.<Foo, Foo>chunk(5)
				.reader(joinedQueueReader(null, null))
				.processor(justLogTheObject())
				.writer(toACsvFileNamed(null))
				.listener(new StepLogListener())
				.build();
	}

	@Bean // crea lista con los registros a ser procesados. Datos falopa
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
					giveMeARoutingProcessor(), // Asigna random "V" o "N" (4 a 1) a modo de clasificador
					inputSize,
					null)                 // para setear que thread autorizó
			);
		}
		return new ListItemReader<>(items);
	}

	public ItemProcessor<Foo, Foo> doNothing()
	{
		return new PassThroughItemProcessor<>();
	}

	@Bean
	public ItemProcessor<Foo, Foo> justLogTheObject() {
		return item -> {
			log.info(item.toString());
			return item;
		};
	}


	public ItemWriter<Foo> toDevNull()
	{
		return items -> {};
	}

	@Bean // Graba en archivo CSV con nombre = [inputSize]. Deben quedar los registros con Foo.totalSize iguales
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
							"totalSize",
							"auth"})
					.build();
	}

	@Bean // Lee de cola visa
	@StepScope
	public JmsSelectorItemReader<Foo> fromVisaQueue(
			@Qualifier("withThisJmsTemplateForVisa") JmsTemplate jmsTemplate,
			@Value("#{stepExecution}") StepExecution stepExecution
	) {
		long selector = stepExecution.getJobExecution().getJobInstance().getInstanceId();
		JmsSelectorItemReader<Foo> jmsSelectorItemReader = new JmsSelectorItemReader<>();

		jmsSelectorItemReader.setJmsTemplate(jmsTemplate);
		jmsSelectorItemReader.setItemType(Foo.class);
		jmsSelectorItemReader.setSelector("jobInstance = " + Long.valueOf(selector).toString());

		return jmsSelectorItemReader;
	}

	@Bean // Lee de cola otros
	@StepScope
	public JmsSelectorItemReader<Foo> fromOthersQueue(
			@Qualifier("withThisJmsTemplateForOthers") JmsTemplate jmsTemplate,
			@Value("#{stepExecution}") StepExecution stepExecution
	) {
		long selector = stepExecution.getJobExecution().getJobInstance().getInstanceId();
		JmsSelectorItemReader<Foo> jmsSelectorItemReader = new JmsSelectorItemReader<>();

		jmsSelectorItemReader.setJmsTemplate(jmsTemplate);
		jmsSelectorItemReader.setItemType(Foo.class);
		jmsSelectorItemReader.setSelector("jobInstance = " + Long.valueOf(selector).toString());

		return jmsSelectorItemReader;
	}

	@Bean // Lee de cola unificada
	@StepScope
	public JmsSelectorItemReader<Foo> joinedQueueReader(
			@Qualifier("withThisJmsTemplate") JmsTemplate jmsTemplate,
			@Value("#{stepExecution}") StepExecution stepExecution
	) {
		long selector = stepExecution.getJobExecution().getJobInstance().getInstanceId();
		JmsSelectorItemReader<Foo> visaItemReader = new JmsSelectorItemReader<>();
		visaItemReader.setJmsTemplate(jmsTemplate);
		visaItemReader.setSelector("jobInstance = " + Long.valueOf(selector).toString());
		return visaItemReader;
	}

	// Writers de los steps

	@Bean // Writer que separa en dos colas los mensaje por el criterio definido en FooClassifier
	@StepScope
	public ClassifierCompositeItemWriter<Foo> splitToDifferentQueues() {
		Classifier<Foo, ItemWriter<? super Foo>> classifier =
				new FooClassifier(visaQueueWriter(null, null),
						othersQueueWriter(null, null));

		return new ClassifierCompositeItemWriterBuilder<Foo>().classifier(classifier).build();
	}

	@Bean // Graba en cola Visa
	@StepScope
	public JmsSelectorItemWriter<Foo> visaQueueWriter(
			@Qualifier("withThisJmsTemplateForVisa") JmsTemplate jmsTemplate,
			@Value("#{stepExecution}") StepExecution stepExecution
	) {
		long selector = stepExecution.getJobExecution().getJobInstance().getInstanceId();
		JmsSelectorItemWriter<Foo> visaItemWriter = new JmsSelectorItemWriter<>();
		visaItemWriter.setJmsTemplate(jmsTemplate);
		visaItemWriter.setSelector(selector);
		return visaItemWriter;
	}

	@Bean // Graba en cola Otros
	@StepScope
	public JmsSelectorItemWriter<Foo> othersQueueWriter(
			@Qualifier("withThisJmsTemplateForOthers") JmsTemplate jmsTemplate,
			@Value("#{stepExecution}") StepExecution stepExecution
	) {
		long selector = stepExecution.getJobExecution().getJobInstance().getInstanceId();
		JmsSelectorItemWriter<Foo> othersItemWriter = new JmsSelectorItemWriter<>();
		othersItemWriter.setJmsTemplate(jmsTemplate);
		othersItemWriter.setSelector(selector);
		return othersItemWriter;
	}

	@Bean // Graba en cola unificada
	@StepScope
	public JmsSelectorItemWriter<Foo> joinedQueueWriter(
			@Qualifier("withThisJmsTemplate") JmsTemplate jmsTemplate,
			@Value("#{stepExecution}") StepExecution stepExecution
	) {
		long selector = stepExecution.getJobExecution().getJobInstance().getInstanceId();
		JmsSelectorItemWriter<Foo> visaItemWriter = new JmsSelectorItemWriter<>();
		visaItemWriter.setJmsTemplate(jmsTemplate);
		visaItemWriter.setSelector(selector);
		return visaItemWriter;
	}

	// Spring Batch shit
	@Bean  // Config de springbatch para lanzar el job de manera asincrona
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
	@Bean // Configuracion cola "Visa" ( Foo.processor = V )
	public JmsTemplate withThisJmsTemplateForVisa() {
		JmsTemplate jmsTemplate = new JmsTemplate(jmsConnectionFactory());
		jmsTemplate.setDefaultDestinationName(this.queue + "Visa");
		jmsTemplate.setMessageConverter(jacksonJmsMessageConverter());
		jmsTemplate.setReceiveTimeout(500);
		return jmsTemplate;
	}

	@Bean // Configuracion cola para "Otros" ( Foo.processor = N )
	public JmsTemplate withThisJmsTemplateForOthers() {
		JmsTemplate jmsTemplate = new JmsTemplate(jmsConnectionFactory());
		jmsTemplate.setDefaultDestinationName(this.queue + "Others");
		jmsTemplate.setMessageConverter(jacksonJmsMessageConverter());
		jmsTemplate.setReceiveTimeout(500);
		return jmsTemplate;
	}

	@Bean // Configuracion cola unificada
	public JmsTemplate withThisJmsTemplate() {
		JmsTemplate jmsTemplate = new JmsTemplate(jmsConnectionFactory());
		jmsTemplate.setDefaultDestinationName(this.queue + "Joined");
		jmsTemplate.setMessageConverter(jacksonJmsMessageConverter());
		jmsTemplate.setReceiveTimeout(500);
		return jmsTemplate;
	}

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
		Job job = ctx.getBean("parallelComplexJob", Job.class);
		//JobParameters jobParameters = new JobParametersBuilder().toJobParameters();

		try {
			JobExecution jobExecution1 = jobLauncher.run(job, new JobParametersBuilder(jex)
					.getNextJobParameters(job).addString("inputSize", "100").toJobParameters());
			JobExecution jobExecution2 = jobLauncher.run(job, new JobParametersBuilder(jex)
					.getNextJobParameters(job).addString("inputSize", "15").toJobParameters());
			//JobExecution jobExecution3 = jobLauncher.run(job, new JobParametersBuilder(jex)
			//		.getNextJobParameters(job).addString("inputSize", "30").toJobParameters());
		} catch (Exception e) {
			e.printStackTrace();
		}

		//SpringApplication.exit(ctx, () -> 0);
	}


}
