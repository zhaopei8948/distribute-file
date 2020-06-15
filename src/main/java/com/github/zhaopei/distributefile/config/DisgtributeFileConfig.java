package com.github.zhaopei.distributefile.config;

import com.github.zhaopei.distributefile.utils.CommonUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DisgtributeFileConfig {

    @Bean
    public Object filePara() {
        return new Object() {

            public String getDir(String queueNames) {
                return CommonUtils.getRandomQueueName(queueNames);
            }
        };
    }
}
