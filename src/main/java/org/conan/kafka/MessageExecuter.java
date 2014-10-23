package org.conan.kafka;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;

/**
 * Created by zhanghang on 2014/9/15.
 */
public interface MessageExecuter {
    public void execute(String message) throws IOException, KeeperException, InterruptedException;
}
