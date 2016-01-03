
import threading
from utils import get_all_text, print_speed, process_print, get_file_list
from globals import setting
from stemmer import PorterStemmer
import os
import time
import re
from multiprocessing import Queue
from multiprocessing.process import Process
from datetime import datetime
import multiprocessing
class pre_worker(object):

    r_stopwords = None    #停用词表
    c_stopwords_lock = threading.Lock()

    def __init__(self,name,task_queue,result_queue):
        self.name = name
        self.r_stemmer = PorterStemmer()
        self.queue = task_queue
        self.queue2 = result_queue
        #载入停词表
        pre_worker.load_stopwords()

    def work(self):
        
        process_print(u'%s开始工作' % (self.name,))

        self.start_time = time.clock()
        count = 0
        while 1:
            content_list = self.queue.get()  #从任务队列里取出n篇文章的所有内容
            if content_list == None:
                self.queue2.put(None)
                break
            count +=len(content_list)
            for content in content_list:
                result_list = self.__parse_doc(content)
                self.queue2.put(result_list)    #将结果存入输出队列中
        process_print(u' %s已退出,共处理了%d个任务' % (self.name,count))

    @staticmethod
    def load_stopwords():
        if pre_worker.r_stopwords == None:
            pre_worker.c_stopwords_lock.acquire()
            try:
                f = open('stopwords.txt')
                content = f.readline().split(',')
                f.close()
                pre_worker.r_stopwords = set(content)
                #print u'停词表载入成功',len(content)
            finally:
                pre_worker.c_stopwords_lock.release()

    def __parse_doc(self,content):
        items = re.findall(setting.c_reg_doc,content)
        result_list = []
        for item in items:
            #获得doc的name
            result = re.search(setting.c_reg_doc_name,item)  
            if result == None:
                continue
            name = result.group(1).strip()
            #获得doc的content
            result = re.search(setting.c_reg_doc_text,item)
            if result == None:
                continue
            text_content = result.group(1)
            term_list = self.__parse_text(text_content)
            if len(term_list) == 0:
                continue
            result_list.append((name,' '.join(term_list)))
        return result_list
    def __parse_text(self,text_content):
        #转换文档正文,分词
        begin = 0
        term_list = []
        for index in range(0,len(text_content)):
            if text_content[index] not in setting.c_valid_chars:
                #词的长度要大于等于最小长度setting.c_min_length
                if index - begin < setting.c_min_length:
                    begin = index + 1
                    continue
                content = text_content[begin:index].lower()
                #获取词根
                content = self.r_stemmer.stem(content,0,len(content) - 1)
                begin = index + 1
                #变为词根之后如果长度太小，依然舍弃
                if len(content) < setting.c_min_length:
                    continue
                #去停用词
                if content in pre_worker.r_stopwords:
                    continue
                term_list.append(content)
            
        return term_list

    def parse_topic(self,filename):
        content = get_all_text(filename)
        items = re.findall(setting.c_reg_topic,content)
        result_list = []
        for item in items:
            result = re.search(setting.c_reg_topic_id,item)
            if result == None:
                continue
            id = result.group(1)
            index = item.index('<title>')
            text_content = re.sub(setting.c_reg_sub,' ',item[index:])
            term_list = self.__parse_text(text_content)
            if len(term_list) == 0:
                continue
            result_list.append((id,' '.join(term_list)))
        return result_list

def add_worker(*args):
    worker = pre_worker(*args)
    worker.work()

class controller(object):
    #预处理控制器
    #src_dir_list:待处理的目录列表
    #dst_doc_name_file:输出的文件名列表
    #dst_term_list_file:输出的词列表
    #worker_count:进程数
    def __init__(self,src_dir_list,dst_doc_name_file,dst_term_list_file,worker_count=None):
        if worker_count == None:
            worker_count = multiprocessing.cpu_count()
        self.map_worker_count = worker_count   #map worker的数量,默认为cpu核数
        self.path_list = src_dir_list  #所有待处理的文件夹列表
        self.worker_process_list = []   #各个worker所占用的进程
        self.total_count = 0        #整个任务的待处理文件总数
        self.file_list = []
        self.task_queue = Queue(20)
        self.result_queue = Queue(100)
        self.name_file = dst_doc_name_file
        self.content_file = dst_term_list_file

    def run(self):
        items = get_file_list(self.path_list)     #取得所有文件列表，读取文件交给了map_workers
        self.file_list = items
        self.total_count = len(items)
        item_step_count = self.total_count / self.map_worker_count  #每个worker分得的任务数量
        print u'文件总数：%d' % self.total_count
        
        for i in range(self.map_worker_count):
            name = 'pre_worker@%d' % (i + 1)
            p = Process(target=add_worker,args=(name,self.task_queue,self.result_queue))
            self.worker_process_list.append(p)
        
        self.start_time = time.clock()
        self.last_refresh_time = time.clock()
        print u'开始时间:',datetime.now()
        for p in self.worker_process_list:
            p.start()
        
        t_reader = threading.Thread(target=self.put_task_thread)
        t_writer = threading.Thread(target=self.get_result_thread)
        t_reader.start()
        t_writer.start()
        print '------------------------'
       
    def put_task_thread(self):
        content_list = []
        count = 0
        for item in self.file_list:
            content_list.append(get_all_text(item))
            count+=1
            if count % 10 == 0:
                self.task_queue.put(content_list)
                content_list = []
        if len(content_list) > 0:
            self.task_queue.put(content_list)
        content_list = None
        for i in range(self.map_worker_count):
            self.task_queue.put(None)

    def get_result_thread(self):
        f_name = open(self.name_file,'w')
        f_content = open(self.content_file,'w')
        count = 0
        start_time = time.clock()
        worker_count = 0
        while 1:
            result_list = self.result_queue.get()     #得到n个结果
            #检测是否有worker退出
            if result_list == None:
                worker_count+=1
                if worker_count < self.map_worker_count:
                    continue
                else:
                    break
            for result in result_list:
                f_name.write('%s\n' % result[0])
                f_content.write('%s\n' % result[1])
            count+=1
            print_speed(start_time,count)
        f_name.close()
        f_content.close()
        print_speed(start_time,count)
        print u'%s已保存' % f_name.name
        print u'%s已保存' % f_content.name
        print u'\n程序执行完毕'


        

