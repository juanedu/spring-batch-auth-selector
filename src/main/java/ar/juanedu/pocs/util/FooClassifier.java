package ar.juanedu.pocs.util;

import org.springframework.batch.item.ItemWriter;
import org.springframework.classify.Classifier;

public class FooClassifier implements Classifier<Foo, ItemWriter<? super Foo>> {

    private ItemWriter<Foo> toAMsgQueueForVisa;
    private ItemWriter<Foo> toAMsgQueueForOthers;

    public FooClassifier(ItemWriter<Foo> toAMsgQueueForVisa, ItemWriter<Foo> toAMsgQueueForOthers) {
        this.toAMsgQueueForVisa = toAMsgQueueForVisa;
        this.toAMsgQueueForOthers = toAMsgQueueForOthers;
    }

    @Override
    public ItemWriter<? super Foo> classify(Foo classifiable) {
            if (classifiable.getProcessor().equals("V"))
                return toAMsgQueueForVisa;
            else
                return toAMsgQueueForOthers;
    }
}
