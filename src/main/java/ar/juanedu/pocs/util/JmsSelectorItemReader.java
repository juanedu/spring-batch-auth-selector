package ar.juanedu.pocs.util;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.jms.JmsItemReader;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import javax.jms.Message;

@Slf4j
public class JmsSelectorItemReader<T> extends JmsItemReader<T> {

    protected String selector;

    public void setSelector(String selector) {
        this.selector = selector;
    }

    @Nullable
    @Override
    @SuppressWarnings("unchecked")
    public T read() {
        if(selector == null || selector.trim().isEmpty()) {
            return super.read();
        }

        if (this.itemType != null && this.itemType.isAssignableFrom(Message.class)) {
            return (T) this.jmsTemplate.receiveSelected(this.selector);
        } else {
            Object result = this.jmsTemplate.receiveSelectedAndConvert(this.selector);
            //log.debug(result.toString());
            if (this.itemType != null && result != null) {
                Assert.state(this.itemType.isAssignableFrom(result.getClass()), "Received message payload of wrong type: expected [" + this.itemType + "]");
            }

            return (T) result;
        }
    }
}
