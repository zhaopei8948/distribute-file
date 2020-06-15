package com.github.zhaopei.distributefile.utils;

import com.github.zhaopei.distributefile.config.DistributeFileFileNameGenerator;
import com.github.zhaopei.distributefile.config.DistributeFileProp;
import org.apache.catalina.util.ParameterMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.endpoint.EventDrivenConsumer;
import org.springframework.integration.endpoint.SourcePollingChannelAdapter;
import org.springframework.integration.file.FileReadingMessageSource;
import org.springframework.integration.file.FileWritingMessageHandler;
import org.springframework.integration.file.filters.SimplePatternFileListFilter;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.support.PeriodicTrigger;
import org.springframework.util.StringUtils;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

public class CommonUtils {

    private static final Log logger = LogFactory.getLog(CommonUtils.class);

    public static void initAdapter() {
        initDirectoryOutboundAdapter();
        initDirectoryInboundAdapter();
    }

    private static void initDirectoryInboundAdapter() {
        DefaultListableBeanFactory defaultListableBeanFactory = SpringContextUtils.getListableBeanFactory();
        DistributeFileProp distributeFileProp = defaultListableBeanFactory.getBean(DistributeFileProp.class);

        if (null == distributeFileProp.getDirectoriesInputList() || distributeFileProp.getDirectoriesInputList().isEmpty()) {
            return;
        }

        Map<String, Object> propertyValueMap = null;
        String[] directoryInfos = null;
        String key = "";
        String suffix = "";
        String directory = null;
        String dir = null;
        String[] directories = null;
        String fileFilter = null;
        Integer periodic = null;
        Integer maxMessagesPrePoll = null;
        Integer minConcurrency = null;
        Integer maxConcurrency = null;
        Integer keepAliveSeconds = null;
        Integer queueCapacity = null;
        String threadNamePrefix = null;
        String channelName = null;
        String pollingChannelAdapterBeanName = null;
        FileReadingMessageSource fileReadingMessageSource = null;
        ThreadPoolTaskExecutor threadPoolTaskExecutor = null;

        for (String inDirInfo : distributeFileProp.getDirectoriesInputList()) {
            directoryInfos = inDirInfo.split("\\|");
            if (directoryInfos.length < 10) {
                continue;
            }

            propertyValueMap = new HashMap<>();
            directory = directoryInfos[0].trim();
            fileFilter = directoryInfos[1].trim();
            periodic = Integer.valueOf(directoryInfos[2].trim());
            maxMessagesPrePoll = Integer.valueOf(directoryInfos[3].trim());
            minConcurrency = Integer.valueOf(directoryInfos[4].trim());
            maxConcurrency = Integer.valueOf(directoryInfos[5].trim());
            keepAliveSeconds = Integer.valueOf(directoryInfos[6].trim());
            queueCapacity = Integer.valueOf(directoryInfos[7].trim());
            threadNamePrefix = directoryInfos[8];
            channelName  = directoryInfos[9];
            directories = directory.split(",");
            key = "directory-";
            for (int i = 0; i < directories.length; i++) {
                dir = directories[i];
                suffix = dir + "-" + i + "-";
                pollingChannelAdapterBeanName = key + "SourcePollingChannelAdapter" + suffix;
                fileReadingMessageSource = new FileReadingMessageSource();
                fileReadingMessageSource.setDirectory(new File(dir));
                fileReadingMessageSource.setFilter(new SimplePatternFileListFilter(fileFilter));
                fileReadingMessageSource.afterPropertiesSet();
                threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
                threadPoolTaskExecutor.initialize();
                threadPoolTaskExecutor.setCorePoolSize(minConcurrency);
                threadPoolTaskExecutor.setMaxPoolSize(maxConcurrency);
                threadPoolTaskExecutor.setKeepAliveSeconds(keepAliveSeconds);
                threadPoolTaskExecutor.setQueueCapacity(queueCapacity);
                threadPoolTaskExecutor.setThreadNamePrefix(threadNamePrefix + suffix);
                threadPoolTaskExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());

                try {
                    defaultListableBeanFactory.getBean(channelName);
                } catch (BeansException e) {
                    createAndregisterBean(DirectChannel.class, channelName, null, null, null);
                }

                propertyValueMap.put("source", fileReadingMessageSource);
                propertyValueMap.put("outputChannelName", channelName);
                propertyValueMap.put("trigger", new PeriodicTrigger(periodic));
                propertyValueMap.put("maxMessagesPerPoll", maxMessagesPrePoll);
                propertyValueMap.put("taskExecutor", threadPoolTaskExecutor);
                createAndregisterBean(SourcePollingChannelAdapter.class, pollingChannelAdapterBeanName, propertyValueMap, null, null);

                ((SourcePollingChannelAdapter) defaultListableBeanFactory.getBean(pollingChannelAdapterBeanName)).start();
            }
        }
    }

    private static void initDirectoryOutboundAdapter() {
        DefaultListableBeanFactory defaultListableBeanFactory = SpringContextUtils.getListableBeanFactory();
        DistributeFileProp distributeFileProp = defaultListableBeanFactory.getBean(DistributeFileProp.class);

        if (null == distributeFileProp.getDirectoriesOutputList() || distributeFileProp.getDirectoriesOutputList().isEmpty()) {
            return;
        }

        String eventDrivenConsumerBeanName = null;
        String fileWritingMessageHandlerBeanName = null;
        List<Object[]> constructorArgList = null;
        Map<String, Object> propertyValueMap = null;
        String[] directoryInfos = null;
        String channelName = null;
        String directory = null;
        String filePrefix = null;
        String fileSuffix = null;
        Expression directoryExpression = null;

        for (String inDirInfo : distributeFileProp.getDirectoriesOutputList()) {
            directoryInfos = inDirInfo.split("\\|");
            if (directoryInfos.length < 4) {
                continue;
            }

            channelName = directoryInfos[0];
            directory = directoryInfos[1];
            filePrefix = directoryInfos[2];
            fileSuffix = directoryInfos[3];
            eventDrivenConsumerBeanName = String.format("%s-%s-eventDrivenConsumer", directory, channelName);
            fileWritingMessageHandlerBeanName = String.format("%s-fileWritingMessageHandler", directory);

            try {
                defaultListableBeanFactory.getBean(channelName);
            } catch (BeansException e) {
                createAndregisterBean(DirectChannel.class, channelName, null, null, null);
            }

            constructorArgList = new ArrayList<>();
            directoryExpression = new SpelExpressionParser().parseExpression("@filePara.getDir('" + directory + "')");
            constructorArgList.add(new Object[]{false, directoryExpression});
//            constructorArgList.add(new Object[]{false, new File(getRandomQueueName(directory))});
            propertyValueMap = new HashMap<>();
            propertyValueMap.put("expectReply", false);
            propertyValueMap.put("fileNameGenerator", new DistributeFileFileNameGenerator(filePrefix, fileSuffix, true));
            propertyValueMap.put("autoCreateDirectory", true);
            propertyValueMap.put("deleteSourceFiles", true);
            createAndregisterBean(FileWritingMessageHandler.class, fileWritingMessageHandlerBeanName, propertyValueMap, null, constructorArgList);

            constructorArgList.clear();
            constructorArgList.add(new Object[]{true, channelName});
            constructorArgList.add(new Object[]{true, fileWritingMessageHandlerBeanName});
            createAndregisterBean(EventDrivenConsumer.class, eventDrivenConsumerBeanName, null, null, constructorArgList);

            ((EventDrivenConsumer) defaultListableBeanFactory.getBean(eventDrivenConsumerBeanName)).start();
        }
    }

    public static void createAndregisterBean(Class clzz, String beanName, Map<String, Object> propertyValueMap,
                                             Map<String, String> propertyReferenceMap,
                                             List<Object[]> constructorArgList) {
        DefaultListableBeanFactory defaultListableBeanFactory = SpringContextUtils.getListableBeanFactory();

        BeanDefinitionBuilder beanDefinitionBuilder = BeanDefinitionBuilder.genericBeanDefinition(clzz);
        if (null != propertyReferenceMap && !propertyReferenceMap.isEmpty()) {
            for (Map.Entry<String, String> entry : propertyReferenceMap.entrySet()) {
                beanDefinitionBuilder.addPropertyReference(entry.getKey(), entry.getValue());
            }
        }

        if (null != propertyValueMap && !propertyValueMap.isEmpty()) {
            for (Map.Entry<String, Object> entry : propertyValueMap.entrySet()) {
                beanDefinitionBuilder.addPropertyValue(entry.getKey(), entry.getValue());
            }
        }

        if (null != constructorArgList && !constructorArgList.isEmpty()) {
            for (Object[] arg : constructorArgList) {
                if (arg.length >= 2) {
                    if ((Boolean) arg[0]) {
                        beanDefinitionBuilder.addConstructorArgReference((String) arg[1]);
                    } else {
                        beanDefinitionBuilder.addConstructorArgValue(arg[1]);
                    }
                }
            }
        }

        defaultListableBeanFactory.registerBeanDefinition(beanName, beanDefinitionBuilder.getBeanDefinition());

    }

    public static void logError(Log log, Throwable t) {
        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        log.error(sw.toString());
    }

    private static int getRandomIndex(int length) {
        return (int) (Math.random() * length);
    }

    public static String getRandomQueueName(String queueNames) {
        if (StringUtils.isEmpty(queueNames)) {
            return null;
        }

        String[] queueNameArr = queueNames.split(",");
        if (queueNameArr.length == 1) {
            return queueNameArr[0];
        }

        return queueNameArr[getRandomIndex(queueNameArr.length)];
    }
}
