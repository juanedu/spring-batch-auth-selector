package ar.juanedu.pocs.springbatchauthselector;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@EnableBatchProcessing
@SpringBootApplication
public class SpringBatchAuthSelectorApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringBatchAuthSelectorApplication.class, args);
	}

}
