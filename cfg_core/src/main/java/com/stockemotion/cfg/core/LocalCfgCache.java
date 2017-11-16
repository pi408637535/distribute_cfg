package com.stockemotion.cfg.core;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by piguanghua on 2017/11/15.
 */
@Slf4j
public class LocalCfgCache {

    private static Properties localProp; //用于访问本地配置
    //Todo 利用私有内部类来进行单利对象
    private static ConcurrentMap<String, String> cachedConfig = new ConcurrentHashMap<>();

    public static void put(String znodeKey, String znodeValue){
        cachedConfig.put(znodeKey, znodeValue);
    }

    public static void remove(String znodeKey){
        cachedConfig.remove(znodeKey);
    }


    public static void setLocalProp(Properties prop) {
        localProp = prop;
    }

    /**
     *  local > memory > zk
     * @param key
     * @param defauleValue
     * @return
     */
    public static String get(String key, String defauleValue){
        String cacheValue = null;
        if (localProp != null && localProp.containsKey(key)) {
            // local prop 1st
            cacheValue = (String) localProp.get(key);
            return cacheValue;
        } else if (cachedConfig.containsKey(key)) {
            // local cache 2nd
            cacheValue = cachedConfig.get(key);
            return cacheValue;
        } else {
            // remote zk 3rd
            cacheValue = CfgZkClient.client.getData(key);
            log.info(">>>>>>>>>> local cache is not found, getDate from zookeeper:[key:{}, value:{}]", key, cacheValue);
            if (StringUtils.isNotBlank(cacheValue)) {
                cachedConfig.put(key, cacheValue);
            }
        }
        if (cacheValue == null) {
            cacheValue = defauleValue;
        }
        return cacheValue;
    }

}
