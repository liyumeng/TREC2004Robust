import Queue
import sys
import time
from multiprocessing.managers import BaseManager

class QueueManager(BaseManager):
    pass

QueueManager.register('get_task_queue')
QueueManager.register('get_result_queue')

server_addr = '198.199.112.63'

print 'Connect to server %s ...' % server_addr

manager = QueueManager(address=(server_addr,10086),authkey='yumengli2015')

manager.connect()

task = manager.get_task_queue()
result = manager.get_result_queue()

for i in range(10):
    try:
        n = task.get()
        print 'run task %d * %d ...' % (n,n)
        r = '%d * %d=%d' % (n,n,n * n)
        time.sleep(1)
        result.put(r)
    except Queue.Empty:
        print 'task queue is empty.'

print 'worker exit.'