package ar.juanedu.pocs.springbatchparallelrun;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@EnableBatchProcessing
@SpringBootApplication
public class SpringBatchAuthParallelApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringBatchAuthParallelApplication.class, args);
	}

}
