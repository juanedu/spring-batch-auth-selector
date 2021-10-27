package ar.juanedu.pocs.util;

import com.google.common.util.concurrent.RateLimiter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class AuthProcessor implements ItemProcessor<Foo, Foo>, StepExecutionListener {

    private RateLimiter rateLimiter;

    private StepExecution stepExecution;

    public AuthProcessor(RateLimiter authRate) {
        this.rateLimiter = authRate;
    }

    @Override
    public Foo process(Foo item) throws Exception {
        rateLimiter.acquire();
        item.setAuth(stepExecution.getStepName() + "-" + Thread.currentThread().getName());
        log.info(item.toString());
        return item;
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        this.stepExecution = stepExecution;
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        return stepExecution.getExitStatus();
    }
}
