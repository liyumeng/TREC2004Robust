import time
from multiprocessing.managers import BaseManager
import random
import Queue

task_queue = Queue.Queue()
result_queue = Queue.Queue()

class QueueManager(BaseManager):
    pass

QueueManager.register('get_task_queue',callable=lambda:task_queue)
QueueManager.register('get_result_queue',callable=lambda:result_queue)

manager = QueueManager(address=('',10086),authkey='yumengli2015')

manager.start()

#必须操作这个Queue，不然就绕过了QueueManager的封装
task = manager.get_task_queue()
result = manager.get_result_queue()

for i in range(10):
    n = random.randint(0,10000)
    print 'put task %d...' % n
    task.put(n)

print 'Try get results...'

for i in range(10):
    r = result.get(timeout=100)
    print 'result: %s' % r

manager.shutdown()