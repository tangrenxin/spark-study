分布式计算模拟-客户端向服务端发送计算任务

实现单点的计算任务分发

真正传输的计算任务是SubTask，而此时的Task就就是一个数据结构，对应spark中的RDD