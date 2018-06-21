package com.kohls.pmdm.batch.master.common;

import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;


public class CommonProperties {
    public String getCollectionSuffix() {
        return null;
    }

    public boolean isEnableFSKUKafka() {
        return false;
    }

    public int getCsTokenCount() {
        return 0;
    }
}
