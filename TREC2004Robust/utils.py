import os
import time
import sys
def rename(old,new):
    if os.path.isfile(new):
        os.remove(new)
    os.rename(old,new)

def makesure_dir(path):
    if os.path.isdir(path) == False:
        os.makedirs(path)
        return False
    return True
def clear_dir(path):
    if os.path.isdir(path):
        [os.remove(os.path.join(path,f)) for f in os.listdir(path)]
    else:
        os.makedirs(path)

def get_all_text(filename):
    f = open(filename)
    content = f.read()
    f.close()
    return content

def print_speed(start_time,count):
    speed = 60 * count / (time.clock() - start_time)
    print '\r',u'完成%d,速度%d个/分钟' % (count,speed),

def get_file_list(path_list):
    file_list = []
    for dir in path_list:
        for parent,dirnames,filenames in os.walk(dir):
            for f in filenames:
                file_list.append(os.path.join(parent,f))
    return file_list

def write_all_text(filename,content):
    f = open(filename,'w')
    f.write(content)
    f.close()

class ProcecssLock(object):
    def __init__(self):
        self.name = 'lock'
        self._f = None
        if os.path.isfile(self.name):
            try:
                os.remove(self.name)
            except:
                pass

    def acquire(self):
        while os.path.isfile(self.name):
            time.sleep(0.1)
        self._f = open(self.name,'w')
    def release(self):
        try:
            self._f.close()
            os.remove(self.name)
        except:
            pass
  
lock = ProcecssLock()      
def process_print(line):
    lock.acquire()
    sys.stdout.write(line + '\n')
    sys.stdout.flush()
    lock.release()