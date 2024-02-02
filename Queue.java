import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.io.*;


class Message {
    String topic, body;
    long timeToLive, birthTime;

    Message(String topic, String body) {
        this.topic = topic;
        this.body = body;
        this.timeToLive = 5000;
        this.birthTime = System.currentTimeMillis();
    }

    Message(String topic, String body, long timeToLive) {
        this.topic = topic;
        this.body = body;
        this.timeToLive = timeToLive;
    }
}

class Status {
    int numMessages, len_messages; 
    long programSize;

    Status(int numMessages, int len_messages, long programSize) {
        this.numMessages = numMessages;
        this.len_messages = len_messages;
        this.programSize = programSize;
    }
}

public class Queue {
    static LinkedList<Message> queue = new LinkedList<Message>();
    static Semaphore writeLock = new Semaphore(1), readLock = new Semaphore(1), mutex = new Semaphore(0);
    static int size = 0;
    static AtomicInteger messagesNum = new AtomicInteger(0), programSize = new AtomicInteger(0);
    static boolean size_limit;

    Queue(int size) {
        this.size = size;
        this.size_limit = true;
    }

    Queue() {
        this.size_limit = false;
    }

    public static void send_message(Message message) {
        writeLock.acquireUninterruptibly();
            if ((messagesNum.get() >= size || programSize.get() + message.body.length() >= size * 10) && size_limit)
                System.out.println("Queue is full");
            else {
                queue.add(message);
                messagesNum.incrementAndGet();
                programSize.addAndGet(message.body.length());
                System.out.println("Produced: " + message.topic + " " + message.body);
                mutex.release();
            }
            writeLock.release();
    }

    public static String get_message() {
        mutex.acquireUninterruptibly();
        readLock.acquireUninterruptibly();
        Message message = queue.removeFirst();
        messagesNum.decrementAndGet();
        programSize.addAndGet(-message.body.length());
        System.out.println("Consumed: " + message.topic + " " + message.body);
        readLock.release();
        return message.topic;
    }

    public static String get_message_with_priority() {
        mutex.acquireUninterruptibly();
        readLock.acquireUninterruptibly();
        writeLock.acquireUninterruptibly();
        int highest = 0;
        for (int i = 1; i < queue.size(); i++)
            if (queue.get(i).timeToLive < queue.get(highest).timeToLive)
                highest = i;
        Message removed = queue.remove(highest); 
        messagesNum.decrementAndGet();
        programSize.addAndGet(-removed.body.length());
        System.out.println("Consumed: " + removed.topic + " " + removed.body);
        writeLock.release();
        readLock.release();
        return removed.topic;
    }

    public static String get_message_nb() {
        readLock.acquireUninterruptibly();
        if (messagesNum.get() > 0) {
            mutex.acquireUninterruptibly();
            Message message = queue.remove();
            messagesNum.decrementAndGet();
            programSize.addAndGet(-message.body.length());
            System.out.println("Consumed non blocking: " + message.topic + " " + message.body);
            readLock.release();
            return message.topic;
        } else
            System.out.println("Queue is empty");
        readLock.release();
        return null;
    }

    public Status stats() {
        readLock.acquireUninterruptibly();
        writeLock.acquireUninterruptibly();
        int queueSize = messagesNum.get(), queueVolume = programSize.get();
        Runtime runtime = Runtime.getRuntime();
        long programMemory = runtime.totalMemory() - runtime.freeMemory();
        writeLock.release();
        readLock.release();
        return new Status(queueSize, queueVolume, programMemory);
    }

    public static void fullQueuePolitics() {
        readLock.acquireUninterruptibly();
        writeLock.acquireUninterruptibly();
        while (queue.size() > size / 2) {
            mutex.acquireUninterruptibly();
            Message removed = queue.remove();
            messagesNum.decrementAndGet();
            programSize.addAndGet(-removed.body.length());
            System.out.println("Removed due to full queue: " + removed.topic + " " + removed.body);
        }
        writeLock.release();
        readLock.release();
    }

    public static void fullQueuePolitics2() {
        mutex.acquireUninterruptibly();
        readLock.acquireUninterruptibly();
        writeLock.acquireUninterruptibly();
        Message removed = queue.remove();
        messagesNum.decrementAndGet();
        programSize.addAndGet(-removed.body.length());
        System.out.println("Removed due to full queue: " + removed.topic + " " + removed.body);
        writeLock.release();
        readLock.release();
    }

    public static void fullQueuePolitics3() {
        mutex.acquireUninterruptibly();
        readLock.acquireUninterruptibly();
        writeLock.acquireUninterruptibly();
        Message removed = queue.removeLast();
        messagesNum.decrementAndGet();
        programSize.addAndGet(-removed.body.length());
        System.out.println("Removed due to full queue: " + removed.topic + " " + removed.body);
        writeLock.release();
        readLock.release();
    }

    public static void fullQueuePolitics4() {
        mutex.acquireUninterruptibly();
        readLock.acquireUninterruptibly();
        writeLock.acquireUninterruptibly();
        Random rand = new Random();
        Message removed = queue.remove(rand.nextInt(size));
        messagesNum.decrementAndGet();
        programSize.addAndGet(-removed.body.length());
        System.out.println("Removed due to full queue: " + removed.topic + " " + removed.body);
        writeLock.release();
        readLock.release();
    }

    public static void saveToFile() {
        readLock.acquireUninterruptibly();
        writeLock.acquireUninterruptibly();
        try {
            FileWriter fw = new FileWriter("messages.txt");
            for (Message m : queue)
                fw.write(m.topic + " " +  m.body + "\n");
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        writeLock.release();
        readLock.release();
    }

    public static void LoadFromFile() {
        readLock.acquireUninterruptibly();
        Scanner scanner = null;
        try {
            File file = new File("messages.txt");
            scanner = new Scanner(file);
        } catch (IOException e) {
            e.printStackTrace();
        }
        while (scanner.hasNextLine()) {
            String[] line = scanner.nextLine().split(" ");
            String str = "";
            for (int i = 1; i < line.length; i++)
                str += line[i] + " ";
            queue.add(new Message(line[0], str));
            mutex.release();
        }
        scanner.close();
        readLock.release();
    }

    static class GarbageCollector implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                readLock.acquireUninterruptibly();
                writeLock.acquireUninterruptibly();
                for (int i = 0; i < queue.size(); i++) {
                    Message message = queue.get(i);
                    if (System.currentTimeMillis() - message.birthTime > message.timeToLive) {
                        queue.remove(i);
                        mutex.acquireUninterruptibly();
                        System.out.println("Removed due to timeout: " + message.topic + " " + message.body);
                    }
                } 
                writeLock.release();
                readLock.release();
            }
        }
    }

    static class Producer implements Runnable {
        Message message;

        Producer(Message message) {
            this.message = message;
        }

        @Override
        public void run() {
            send_message(message);
        }
    }

    static class Consumer implements Runnable {
        @Override
        public void run() {
            get_message();
        }
    }

    static class ConsumerNB implements Runnable {
        @Override
        public void run() {
            get_message_nb();
        }
    }

    static class ConsumerWithPriority implements Runnable {
        @Override
        public void run() {
            get_message_with_priority();
        }
    }

    public static void main(String[] args) {
        Queue q = new Queue(2);
        Producer p = new Producer(new Message("1", "message 1", 2));
        Producer p2 = new Producer(new Message("2", "message 2"));
        Producer p3 = new Producer(new Message("3", "message 3"));
        Producer p4 = new Producer(new Message("4", "message 4", 1));
        Consumer c = new Consumer();
        Consumer c2 = new Consumer();
        Consumer c3 = new Consumer();
        Consumer c7 = new Consumer();
        ConsumerNB c4 = new ConsumerNB();
        ConsumerNB c6 = new ConsumerNB();
        ConsumerWithPriority c5 = new ConsumerWithPriority();
        Thread thread = new Thread(p);
        Thread thread2 = new Thread(p2);
        Thread thread3 = new Thread(c);
        Thread thread4 = new Thread(c2);
        Thread thread5 = new Thread(c3);
        Thread thread6 = new Thread(p3);
        Thread thread7 = new Thread(c4);
        Thread thread8 = new Thread(p4);
        Thread thread9 = new Thread(c5);
        Thread thread10 = new Thread(c6);
        Thread thread11 = new Thread(c7);


        // full queue size
        // thread.start();
        // thread2.start();
        // thread6.start();

        // one consumer and producer
        // thread3.start();
        // try {
        //     Thread.sleep(1000);
        // } catch (InterruptedException e) {
        //     e.printStackTrace();
        // }
        // thread.start();

        // two consumers and one producer
        // thread3.start();
        // thread4.start();
        // try {
        //     Thread.sleep(1000);
        // } catch (InterruptedException e) {
        //     e.printStackTrace();
        // }
        // thread.start();

        // Non blocking consumer
        // thread7.start();

        // Unbounded queue
        // Queue uq = new Queue();
        // Random random = new Random();
        // for (int i = 0; i < 10; i++) {
        //     Producer pp = new Producer(new Message(Integer.toString(random.nextInt(100)), Integer.toString(random.nextInt(100))));
        //     Thread tt = new Thread(pp);
        //     tt.start();
        // }

        // Time to live
        // GarbageCollector gc = new GarbageCollector();
        // Thread tgc = new Thread(gc);
        // tgc.start();
        // thread.start(); // timeToLive = 2s
        // try {
        //     Thread.sleep(3000);
        // } catch (InterruptedException e) {
        //     e.printStackTrace();
        // }
        // thread3.start();

        // Volume full queue
        // Queue qv = new Queue(1);
        // Producer pp = new Producer(new Message("1", "message 111111111111111111111111111111111"));
        // Thread tt = new Thread(pp);
        // tt.start();

        // load from file
        // q.LoadFromFile();
        // thread3.start();
        // thread4.start();

        // save to file
        // thread.start();
        // thread2.start();
        // try {
        //     Thread.sleep(1000);
        // } catch (InterruptedException e) {
        //     e.printStackTrace();
        // }
        // q.saveToFile();

        // full queue size with politics 1 (half)
        // thread.start();
        // thread2.start();
        // q.fullQueuePolitics();
        // // q.fullQueuePolitics2();
        // // q.fullQueuePolitics3();
        // // q.fullQueuePolitics4();
        // try {
        //     Thread.sleep(1000);
        // } catch (InterruptedException e) {
        //     e.printStackTrace();
        // }
        // thread6.start();

        // priority queue
        // thread.start();
        // thread8.start();
        // thread9.start();

        // two non blocking consumers and one producer
        // thread.start();
        // try {
        //     Thread.sleep(1000);
        // } catch (InterruptedException e) {
        //     e.printStackTrace();
        // }
        // thread7.start();
        // thread10.start();

        // couple of consumers then producers and consumers again
        // Queue qt = new Queue(5);
        // thread3.start();
        // thread4.start();
        // thread5.start();
        // try {
        //     Thread.sleep(1000);
        // } catch (InterruptedException e) {
        //     e.printStackTrace();
        // }
        // thread.start();
        // thread2.start();
        // thread6.start();
        // thread8.start();
        // thread11.start();
    }
}
