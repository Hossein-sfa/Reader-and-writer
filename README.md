# Reader-and-writer
Os project that has concurrent producers and consumers which send and receive messages in a queue with java language.
three semaphores and two atomic integers are used for handling critical sections
also has a garbage collector that runs every 500ms and delete messages that their timetolive has ended.
senders are non blocking and recivers are both blocking and non blocking.
receivers can also have a priority.
queue can be bounded an unbounded so on, there are several politics for full queue.
There is also load and save to file feature.
