import os
import threading
from multiprocessing.queues import Queue
import time
from Queue import Full, Empty
import thread
import sys
import re
from multiprocessing.process import Process
from utils import print_speed, get_all_text,get_file_list, rename
from stemmer import PorterStemmer
from globals import setting
import struct
reload(sys)
sys.setdefaultencoding('utf-8')

class IOWorker(object):
    def __init__(self,file_list,buffer_size=1000,mode='r'):
        if mode == 'w':
            for f in file_list:
                if os.path.isfile(f):
                    print u'%s 已存在，是否覆盖？（y/n）' % f,
                    info = raw_input()
                    if info != 'y':
                        return
                    else:
                        break
        self._file_list = [open(f,mode) for f in file_list]
        self.is_open = True
        self.buffer_size = buffer_size

    def GetLines(self):
        result = []
        i = 0
        if self.is_open == False:
            return result

        while i < self.buffer_size:
            i+=1
            data = [f.readline() for f in self._file_list]
            if data.count('') == 0:
                result.append(tuple([item[:-1] for item in data]))
            else:
                self.Close()
                break
        return result

    def SaveLines(self,item_list):
        for item in item_list:
            i = 0
            for f in self._file_list:
                f.write('%s\n' % item[i])
                i+=1
    
    def Close(self):
        if self.is_open:
            for f in self._file_list:
                f.close()
            self.is_open = False
    
    @staticmethod
    def SaveText(filename,content):
        f = open(filename,'w')
        f.write(content)
        f.close()

class MapWorker(object):
    def __init__(self):
        pass

    def run(self,task_raw_data):
        return [self.__run_one(item) for item in task_raw_data]

    def __run_one(self,item):
        dict = {}
        terms = item[1][:-1].split(' ')
        for term in terms:
            dict[term] = dict.get(term,0) + 1
        term_list = dict.items()
        term_list.sort()
        return (item[0],term_list)

##如果想写多进程，Manager的方法要写成静态的才可以
class Manager(object):
    def __init__(self,content_file,name_file,buffer_size=1000):
        super(Manager,self).__init__()

        self.finished = 0
        self.content_file = content_file
        self.name_file = name_file
        self.task_queue = Queue(buffer_size)
        self.result_queue = Queue(buffer_size)
    
    def run(self):
        file_list = [self.name_file,self.content_file]
        t_reader = threading.Thread(target=self.IOReadThread,args=(file_list,self.task_queue))
        t_writer = threading.Thread(target = self.IOWriteThread,args = ([file + '.multi' for file in file_list],self.result_queue))
        t_worker = threading.Thread(target = self.MapWorkerThread)
        self.thread_list = [t_reader,t_writer,t_worker]
        for t in self.thread_list:
            t.start()

    #用不了
    def runInMultiProcess(self):
        file_list = [self.name_file,self.content_file]
        p_reader = Process(target=self.IOWriteThread,args=(file_list,self.task_queue))
        p_writer = Process(target = self.IOReadThread,args = ([file + '.multi' for file in file_list],self.result_queue))
        p_worker = Process(target = self.MapWorkerThread)
        self.process_list = [p_reader,p_writer,p_worker]
        for p in self.process_list[:-2]:
            p.start()

    def PutTask(self,task_raw_data,task_queue):
        #return self.task_queue.put(task_raw_data)
        while 1:
            try:
                task_queue.put(task_raw_data,False)
                break
            except Full:
                while task_queue.qsize() > task_queue._maxsize / 2:
                    time.sleep(1)

    def GetResult(self,result_queue):
        #return self.result_queue.get()
        while 1:
            try:
                result_raw_data = result_queue.get(False)
                break
            except Empty:
                while result_queue.qsize() < result_queue._maxsize / 2:
                    time.sleep(1)
        return result_raw_data

    def IOReadThread(self,file_list,task_queue):
        worker = IOWorker(file_list)
        while 1:
            task_raw_data = worker.GetLines()
            self.PutTask(task_raw_data,task_queue)
            if len(task_raw_data) == 0:
                break
        print u'任务全部添加完毕'

    def IOWriteThread(self,file_list,result_queue):
        worker = IOWorker(file_list,mode='w')
        status = False
        while 1:
            result_raw_data = self.GetResult(result_queue)
            if len(result_raw_data) == 0:
                break
            worker.SaveLines(result_raw_data)
            self.finished+=len(result_raw_data)
        worker.Close()
        print u'全部保存完毕'

    def MapWorkerThread(self):
        worker = MapWorker()
        while 1:
            task_raw_data = self.task_queue.get()
            if len(task_raw_data) == 0:
                self.result_queue.put([])
                break
            result = worker.run(task_raw_data)
            result_raw_data = [Converter.MapResultToString(item) for item in result]
            self.result_queue.put(result_raw_data)
        print u'任务全部执行完毕'

class ReduceWorker(object):
    def __init__(self):
        pass

    #reduce阶段要传入一整组的数据，list of (docid,term_list)
    #传出list of (term,docid_list)
    def run(self,item_list):
        term_dict = {}
        term_count = {}
        for item in item_list:
            term_list = item[1]
            for term in term_list:
                key = term[0]
                if term_dict.has_key(key) == False:
                    term_dict[key] = []
                term_dict[key].append(item[0])
                term_count[key] = term_count.get(key,0) + int(term[1])
        term_list = []
        for key in term_dict:
            term_list.append((key,term_count[key],term_dict[key]))
        term_list.sort()
        return term_list
    def save(self):
        pass

class Converter(object):
    @staticmethod
    def ToDocList(task_raw_data,start_id):
        doc_list = []
        for content in task_raw_data:
            items = content[0].split(',')
            term_list = []
            for item in items:
                if item == '':
                    continue
                data = item.split(':')
                term_list.append(tuple(data))
            doc_list.append((start_id,term_list))
            start_id+=1
        return doc_list
    @staticmethod
    def ToTermList(line):
        items = line.split(',')
        term_list = []
        for item in items:
            if item == '':
                continue
            data = item.split(':')
            term_list.append(tuple(data))
        return term_list
    @staticmethod
    def MapResultToString(item):
        term_list = item[1]
        tmp = ['%s:%s' % term for term in term_list]
        return (item[0],','.join(tmp))
    @staticmethod
    def TermInverterToString(term_inverter_list):
        result = ['%s,%d,%d,%s' % (item[0],item[1],len(item[2]),','.join(str(i) for i in item[2])) for item in term_inverter_list]
        return '\n'.join(result)

#一个通用的外部归并类，可以把file_list的文件夹，
#按照每行首个逗号前的值作为key，
#第二个逗号前的数值相加，
#第三个逗号前的数值相加，
#后面的值作为value直接连接归并
#相同key后面的value值连接在一起
#要求file_list中的所有文件都是有序的
#输出是有序的
class Merger(object):
    def __init__(self,file_list,merged_file_name):
        self.file_list = file_list
        self.merged_file = open(merged_file_name,'w')
        self.has_prefix = False

    def run(self):
        reader_list = [MergeReader(filename) for filename in self.file_list]
        length = len(reader_list)
        for reader in reader_list:
            reader.GetKeyValue()
        reader_list = [reader for reader in reader_list if reader.keyValue is not None]
        total = 0
        start_time = time.clock()
        while len(reader_list) > 0:
            min_reader_list = [reader_list[0]]
            for reader in reader_list[1:]:
                if reader.keyValue[0] < min_reader_list[0].keyValue[0]:
                    min_reader_list = [reader]
                elif reader.keyValue[0] == min_reader_list[0].keyValue[0]:
                    min_reader_list.append(reader)
            key = min_reader_list[0].keyValue[0]
            count = sum(reader.keyValue[1] for reader in min_reader_list)
            count2 = sum(reader.keyValue[2] for reader in min_reader_list)
            value = ','.join([reader.keyValue[3] for reader in min_reader_list])
            if key == 'zzzz':
                pass
            self.merged_file.write('%s,%d,%d,%s\n' % (key,count,count2,value))

            for reader in min_reader_list:
                reader.GetKeyValue()
                if reader.keyValue is None:
                    reader_list = [item for item in reader_list if item.keyValue is not None]

            total+=1
            if total % 10000 == 0:
                print_speed(start_time,total)
        self.merged_file.close()
        print_speed(start_time,total)
        print total,u'保存完毕'

class MergeReader(object):
    def __init__(self,filename,buffer_size=500):
        self.file = open(filename)
        self.buffer_size = buffer_size
        self.count = 0
        self.buffer_index = buffer_size
        self.buffer = []
        self.is_open = True
        self.keyValue = None

    def GetKeyValue(self):
        if self.buffer_index >= self.buffer_size:
            self.buffer_index = 0
            raw_data = self.__readlines(self.buffer_size)
            self.buffer = [MergeReader.ToKeyValue(raw_content) for raw_content in raw_data]
        if self.buffer_index >= len(self.buffer):   #文件已读到末尾
            self.keyValue = None
            return None
        pair = self.buffer[self.buffer_index]
        self.count+=1
        self.buffer_index+=1
        self.keyValue = pair
        return pair

    def __readlines(self,size):
        raw_data = []
        if self.is_open == False:
            return raw_data
        i = 0
        while i < size:
            tmp = self.file.readline()
            if tmp == '' or tmp == None:
                self.file.close()
                self.is_open = False
                break
            if tmp.startswith(',') or tmp == '\n':
                continue
            i+=1
            if tmp.endswith('\n'):
                raw_data.append(tmp[:-1])
            else:
                raw_data.append(tmp)
        return raw_data
    
    @staticmethod
    def ToKeyValue(content):
        index = content.index(',')
        index2 = content.index(',',index + 1)
        index3 = content.index(',',index2 + 1)
        return (content[:index],int(content[index + 1:index2]),int(content[index2 + 1:index3]),content[index3 + 1:])
        
class Searcher(object):
    def __init__(self,inverter_file,vector_file,content_file):
        self.f_term = open(inverter_file)
        self.f_doc = open(vector_file,'rb')

        #self.f_content = open(content_file)
        self.content_index = get_all_text(content_file + '.index').split(',')
        self.term_index,self.term_name = self.loadTermIndex(inverter_file + '.index')
        self.doc_index = get_all_text(vector_file + '.index').split(',')
        print u'索引载入完毕'

    def loadTermIndex(self,filename):
        dict = {}
        items = get_all_text(filename).split(',')
        count = 0
        term_name_list = []
        for item in items:
            if item == '':
                continue
            data = item.split(':')
            dict[data[0]] = (str(count),data[1]) #(id,line_posi)
            term_name_list.append(data[0])
            count +=1
        return dict,term_name_list

    def search(self,term):
        docid_list = []
        if self.term_index.has_key(term):
            id = self.term_index[term][0]
            line_num = self.term_index[term][1]
            self.f_term.seek(int(line_num))
            content = self.f_term.readline()
            if content.endswith('\n'):
                content = content[:-1]
            index = content.index(',')
            index2 = content.index(',',index + 1)
            index3 = content.index(',',index2 + 1)
            docid_list = content[index3 + 1:].split(',')
        return docid_list
    def searchByDocId(self,doc_id):
        line_num = self.doc_index[int(doc_id)]
        self.f_doc.seek(long(line_num))
        count = self.get_int()
        term_list = []
        for i in range(count):
            id = self.get_int()
            term_list.append((self.term_name[id],self.get_float()))
        return term_list

    def search_value(self,term):
        vector_list = []
        if self.term_index.has_key(term):
            id = self.term_index[term][0]
            line_num = self.term_index[term][1]
            self.f_term.seek(int(line_num))
            content = self.f_term.readline()
            index = content.index(',')
            index2 = content.index(',',index + 1)
            index3 = content.index(',',index2 + 1)
            vector_list = [ (docid,self.getVector(int(docid),id)) for docid in content[index3 + 1:-1].split(',') if docid is not '']
        return vector_list

    def getVector(self,doc_id,term_id):
        line_num = self.doc_index[doc_id]
        self.f_doc.seek(long(line_num))
        content = self.f_doc.readline()
       
        items = content[:-1].split(',')
        for item in items:
            data = item.split(':')
            if data[0] == term_id:
                return float(data[1])
        print u'未在doc_id=%d中找到term_id=%s' % (doc_id,term_id)
        print content
        return None

    def get_int(self):
        return struct.unpack('i',self.f_doc.read(4))[0]
    def get_float(self):
        return struct.unpack('f',self.f_doc.read(4))[0]

    def Close(self):
        self.f_doc.close()
        self.f_term.close()

class Searcher2(object):
    def __init__(self,term_vector_file,term_name_file):
        self.f_term = open(term_vector_file)
        self.f_index = open(term_vector_file + '.index','r')
        term_name_list = get_all_text(term_name_file).split('\n')
        self.term_names = term_name_list
        term_vector = [tuple(item.split(':')) for item in get_all_text(term_vector_file + '.index').split(',') if item is not '']
        term_dict = {}
        for item in term_vector:
            term_dict[term_name_list[int(item[0])]] = long(item[1])
        self.term_index = term_dict
        print u'索引载入完毕'

    def search(self,term):
        doc_list = []
        if self.term_index.has_key(term):
            line_num = self.term_index[term]
            self.f_term.seek(int(line_num))
            content = self.f_term.readline()
            if content.endswith('\n'):
                content = content[:-1]
            index = content.index(',')
            doc_list = [tuple(item.split(':')) for item in content[index + 1:].split(',')]
        return doc_list

    def get_int(self):
        return struct.unpack('i',self.f_doc.read(4))[0]
    def get_float(self):
        return struct.unpack('f',self.f_doc.read(4))[0]
    def loadDocNames(self,filename):
        self.doc_names = get_all_text(filename).split('\n')
    def Close(self):
        self.f_index.close()
        self.f_term.close()
        