package com.github.zhaopei.distributefile.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@ConfigurationProperties(prefix = "distributefile")
@Data
public class DistributeFileProp {

    private List<String> directoriesInputList;

    private List<String> directoriesOutputList;
}
