package com.stockemotion.cfg.core;

import com.stockemotion.cfg.utils.Environment;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by piguanghua on 2017/11/16.
 */
@Slf4j
public class CfgZkClient implements Watcher {

    private static String deployenvPath = Environment.deployenv;
    //Todo 没懂
    public static CfgZkClient client = new CfgZkClient();	// 注意静态变量,初始化顺序

    private ZooKeeper zooKeeper;

    public CfgZkClient() {
        try {
            this.zooKeeper = new ZooKeeper(Environment.zkserver, 2000, this);
            this.create(deployenvPath);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     *  循环创建,因为parentPath不存在会保存
     * @param znodePath
     * @return
     */
    private Stat create(String znodePath){
        try {
            Stat stat = this.zooKeeper.exists(znodePath, true);
            if (stat == null) {
                //  create parent znodePath
                String parentPath = generateParentPath(znodePath);
                if (StringUtils.isNotBlank(parentPath)) {
                    Stat parentStat = this.zooKeeper.exists(parentPath, true);
                    if (parentStat == null) {
                        this.create(parentPath);
                    }
                }
                zooKeeper.create(znodePath, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            return this.zooKeeper.exists(znodePath, true);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    //Todo 不知道有什么作用。 好像是分隔啥东西
    private String generateParentPath(String znodePath){
        String parentPath = null;
        if (znodePath != null && znodePath.lastIndexOf("/") != -1 && znodePath.lastIndexOf("/") > 0) {
            parentPath = znodePath.substring(0, znodePath.lastIndexOf("/"));
        }
        return parentPath;
    }



    @Override
    public void process(WatchedEvent event) {
        try {
            log.info(">>>>>>>>>> watcher:{}", BeanUtils.describe(event));
            Event.EventType eventType = event.getType();

            if (eventType == Event.EventType.None) {
                // TODO
            }else if (eventType == Event.EventType.NodeCreated) {
                String znodePath = event.getPath();
                this.zooKeeper.exists(znodePath, true);	// add One-time trigger, ZooKeeper的Watcher是一次性的，用过了需要再注册

                String znodeKey = generateZnodeKeyFromPath(znodePath);
                if (znodeKey == null) {
                    return;
                }
                String znodeValue = this.getData(znodeKey);

                LocalCfgCache.put(znodeKey, znodeValue);
                String localValue = LocalCfgCache.get(znodeKey, null);

                log.info(">>>>>>>>>> 新增配置：zk:[{}:{}]", new Object[]{znodeKey, znodeValue});
                log.info(">>>>>>>>>> 新增配置：local:[{}:{}]", new Object[]{znodeKey, localValue});
            } else if (eventType == Event.EventType.NodeDeleted) {
                String znodePath = event.getPath();
                this.zooKeeper.exists(znodePath, true);

                String znodeKey = generateZnodeKeyFromPath(znodePath);
                String znodeValue = this.getData(znodeKey);

                LocalCfgCache.remove(znodeKey);
                String localValue = LocalCfgCache.get(znodeKey, null);

                log.info(">>>>>>>>>> 删除配置：zk:[{}:{}]", new Object[]{znodeKey, znodeValue});
                log.info(">>>>>>>>>> 删除配置：local:[{}:{}]", new Object[]{znodeKey, localValue});
            } else if (eventType == Event.EventType.NodeDataChanged) {
                String znodePath = event.getPath();
                this.zooKeeper.exists(znodePath, true);

                String znodeKey = generateZnodeKeyFromPath(znodePath);
                String znodeValue = this.getData(znodeKey);

                LocalCfgCache.put(znodeKey, znodeValue);
                String localValue = LocalCfgCache.get(znodeKey, null);

                log.info(">>>>>>>>>> 更新配置:zk：[{}:{}]", new Object[]{znodeKey, znodeValue});
                log.info(">>>>>>>>>> 更新配置：local:[{}:{}]", new Object[]{znodeKey, localValue});
            } else if (eventType == Event.EventType.NodeChildrenChanged) {
                // TODO
            }

        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }
    }

    public Stat setData(String znodeKey, String znodeValue) {
        String znodePath = this.generateZnodePathFromKey(znodeKey);
        try {
            Stat stat = this.zooKeeper.exists(znodePath, true);
            if (stat == null) {
                this.create(znodePath);
                stat = this.zooKeeper.exists(znodePath, true);
            }
            return zooKeeper.setData(znodePath, znodeValue.getBytes(),stat.getVersion());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }



    private String generateZnodeKeyFromPath(String znodePath){
        if (znodePath.length() <= deployenvPath.length()) {
            return null;
        }
        return znodePath.substring(deployenvPath.length()+1, znodePath.length());
    }

    public String getData(String znodeKey){
        String znodePath = this.generateZnodePathFromKey(znodeKey);
        String znodeValue = null;
        try {
            Stat stat = this.zooKeeper.exists(znodePath, true);
            if (stat != null) {
                byte[] resultData = this.zooKeeper.getData(znodePath, this, null);
                if (resultData != null) {
                    znodeValue = new String(resultData);
                }
            } else {
                log.info(">>>>>>>>>> znodeKey[{}] not found.", znodeKey);
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return znodeValue;
    }

    public Map<String, String> getAllData(){
        Map<String, String> addData = new HashMap<String, String>();
        try {
            List<String> nodeNameList = this.zooKeeper.getChildren(deployenvPath, true);
            if (CollectionUtils.isNotEmpty(nodeNameList)) {
                for (String znodeKey : nodeNameList) {
                    String znodeValue = this.getData(znodeKey);
                    addData.put(znodeKey, znodeValue);
                }
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return addData;
    }

    private String generateZnodePathFromKey(String ZnodeKey){
        return deployenvPath + "/" + ZnodeKey;
    }

}
