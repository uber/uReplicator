package com.uber.stream.kafka.mirrormaker.controller.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * @Author: Tboy
 */
public class FileUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileUtils.class);

    public static void mkdirQuietly(String dir){
        try {
            File dirPath = new File(dir);
            if(!dirPath.exists()){
                boolean result = dirPath.mkdirs();
                LOGGER.info("mkdir : {} {}", dir, result ? "success" : "fail");
            }
        } catch (Exception ex){
            //NOP
        }
    }
}
