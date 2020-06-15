package com.github.zhaopei.distributefile;

import com.github.zhaopei.distributefile.utils.CommonUtils;
import com.github.zhaopei.distributefile.utils.SpringContextUtils;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

@SpringBootApplication
public class DistributeFileApplication {

    public static void main(String[] args) {
        ApplicationContext applicationContext = SpringApplication.run(DistributeFileApplication.class, args);
        SpringContextUtils.setApplicationContext(applicationContext);
        CommonUtils.initAdapter();
    }

}
