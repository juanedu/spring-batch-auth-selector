package ar.juanedu.pocs.util;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;

public class StepLogListener implements StepExecutionListener {

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        String jobName = stepExecution.getJobExecution().getJobInstance().toString();
        System.out.println(jobName + "." + stepExecution.getStepName() + " has begun!");
    }

    @AfterStep
    public ExitStatus afterStep(StepExecution stepExecution) {
        String jobName = stepExecution.getJobExecution().getJobInstance().toString();
        System.out.println(jobName + "." + stepExecution.getStepName() + " has ended!");
        return stepExecution.getExitStatus();
    }

}
