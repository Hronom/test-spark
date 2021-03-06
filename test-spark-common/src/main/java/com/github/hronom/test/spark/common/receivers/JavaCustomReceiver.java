package com.github.hronom.test.spark.common.receivers;

import com.github.hronom.test.spark.common.pools.RandomStringGeneratorPool;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

public class JavaCustomReceiver extends Receiver<String> {
    private String
        stringPattern
        = "Ccn!CCccCn!cccccccCn!!Ccccc!cc!ccccc!cccc!!Cc!C!C!Ccn!CCccCn!cccccccCn!!Ccccc!cc!ccccc!cccc!!Cc!C!C!Ccn!CCccCn!cccccccCn!!Ccccc!cc!ccccc!cccc!!Cc!C!C!Ccn!CCccCn!cccccccCn!!Ccccc!cc!ccccc!cccc!!Cc!C!C!Ccn!CCccCn!cccccccCn!!Ccccc!cc!ccccc!cccc!!Cc!C!C!Ccn!CCccCn!cccccccCn!!Ccccc!cc!ccccc!cccc!!Cc!C!C!";

    public JavaCustomReceiver() {
        super(StorageLevel.MEMORY_AND_DISK_2());
    }

    public void onStart() {
        // Start the thread that receives data over a connection.
        Thread thread = new Thread() {
            @Override
            public void run() {
                receive();
            }
        };
        thread.start();
    }

    public void onStop() {
        // There is nothing much to do as the thread calling receive() is designed to stop by itself
        // if isStopped() returns false.
    }

    private void receive() {
        while (!isStopped()) {
            store(RandomStringGeneratorPool.getGenerator().generateFromPattern(stringPattern));
        }
    }
}
