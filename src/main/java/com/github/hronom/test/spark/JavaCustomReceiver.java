package com.github.hronom.test.spark;

import net.moznion.random.string.RandomStringGenerator;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

public class JavaCustomReceiver extends Receiver<String> {
    private RandomStringGenerator generator = new RandomStringGenerator();
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

    /** Create a socket connection and receive data until receiver is stopped */
    private void receive() {
        store(generator.generateFromPattern(stringPattern));
    }
}
