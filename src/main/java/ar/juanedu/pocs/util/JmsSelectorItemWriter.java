package ar.juanedu.pocs.util;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.jms.JmsItemWriter;
import org.springframework.jms.core.JmsOperations;

import java.util.List;

@Slf4j
public class JmsSelectorItemWriter<T> extends JmsItemWriter<T> {

    // Agrego el jmsTemplate porque es private en la superclase
    // En el Reader es protected ¬¬
    private JmsOperations jmsTemplate;
    protected Long selector;

    public void setSelector(Long selector) {
        this.selector = selector;
    }

    @Override
    public void setJmsTemplate(JmsOperations jmsTemplate) {
        this.jmsTemplate = jmsTemplate;
    }

    @Override
    public void write(List<? extends T> items) throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug("Writing to JMS with " + items.size() + " items.");
        }

        for (T item : items) {

            jmsTemplate.convertAndSend(item, messagePostProcessor -> {
                messagePostProcessor.setLongProperty("jobInstance", selector);
                return messagePostProcessor;
            });
        }
    }
}
