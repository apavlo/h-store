package edu.brown.stream;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class BatchProducer implements Runnable {

    BlockingQueue<Tuple> queue = new LinkedBlockingQueue<Tuple>();
    private BlockingQueue<Batch> batchQueue;

    public BatchProducer(BlockingQueue<Batch> q) {
        this.batchQueue = q;
    }

    @Override
    public void run() {
        long success_count = 0;

        long batchInterval = 1000; // FIXME, this parameter can be customized

        try {

            try {
                // this.preProcessBenchmark(icc.client);
            } catch (Exception e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }

            long batchid = 0;
            boolean finishOperation = false;

            long currentTimeStamp = System.currentTimeMillis();
            long nextTimeStamp = currentTimeStamp;

            do {
                currentTimeStamp = nextTimeStamp;
                nextTimeStamp = currentTimeStamp + batchInterval;

                // create new batch
                Batch batch = new Batch();
                batch.setID(batchid++);

                finishOperation = false;
                // get all the tuples in a batch interval
                do {
                    // 1. get tuple, and if it is not null then add it to batch
                    try {
                        // 1.1 get the tuple from queue.
                        // question: if queue is empty what happens
                        Tuple tuple = this.queue.take();

                        if (tuple == null || tuple.getFieldLength() == 0) {
                            System.out.println("Info - BatchProducer : encounter the last empty tuple");
                            finishOperation = true;
                            batchQueue.put(batch);
                            break;
                        }

                        // 1.2 add the tuple to batch
                        long current = System.currentTimeMillis();
                        tuple.addField("TIMESTAMP", current);

                        batch.addTuple(tuple);

                        //
                        if (current >= nextTimeStamp) {
                            // put this batch into batch queue
                            batchQueue.put(batch);
                            // break to next interval
                            break;
                        }
                    } catch (Exception ex) {
                        System.out.println("Queue get error: " + ex.getMessage());
                        Throwable cause = ex.getCause();
                        if (cause != null) {
                            System.out.println("Error cause: " + cause.getMessage());
                        }
                    }

                } while (true);

                // print for debugging
                System.out.println("Batch-" + batch.getID() + " : " + currentTimeStamp + " - #tuples : " + batch.getSize());

                if (finishOperation == true)
                    break;

            } while (true);

        } finally {
            try {
                // put last empty batch
                Batch batch = new Batch();
                this.batchQueue.put(batch);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

}