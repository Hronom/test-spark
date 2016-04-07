package com.github.hronom.test.spark.common.receivers;

import com.github.hronom.test.spark.common.pools.RandomStringGeneratorPool;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

public class JavaCustomWithCountReceiver extends Receiver<String> {
    private final String stringPattern = "Ccn!CCccCn!cccccccCn!!Ccccc!cc!ccccc!cccc!!Cc!C!C!Ccn!CCccCn!cccccccCn!!Ccccc!cc!ccccc!cccc!!Cc!C!C!Ccn!CCccCn!cccccccCn!!Ccccc!cc!ccccc!cccc!!Cc!C!C!Ccn!CCccCn!cccccccCn!!Ccccc!cc!ccccc!cccc!!Cc!C!C!Ccn!CCccCn!cccccccCn!!Ccccc!cc!ccccc!cccc!!Cc!C!C!Ccn!CCccCn!cccccccCn!!Ccccc!cc!ccccc!cccc!!Cc!C!C!";
    private final long countOfReceivedInfo;

    public JavaCustomWithCountReceiver(long countOfReceivedInfoArg) {
        super(StorageLevel.MEMORY_AND_DISK_2());
        countOfReceivedInfo = countOfReceivedInfoArg;
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
        for (long i = 0; i < countOfReceivedInfo; i++) {
            store(RandomStringGeneratorPool.getGenerator().generateFromPattern(stringPattern));
        }
    }
}
