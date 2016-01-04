import time
from workers import Manager,IOWorker, MapWorker, ReduceWorker, Converter,Merger,MergeReader, Searcher, Searcher2
import os
from utils import makesure_dir, clear_dir, write_all_text, get_all_text
from models import  ContentFile, Topic
from math import log, sqrt
from utils import print_speed,get_file_list
import platform
from multiprocessing.process import Process
from pre_dealers import controller, pre_worker
import struct
import gc
from operator import itemgetter

N = 523248
TOPICN = 250


def readInt(f):
    return struct.unpack('i',f.read(4))[0]

def readFloat(f):
    return struct.unpack('f',f.read(4))[0]

#预处理
def PreDealThread(src_dir_list,dst_doc_name_file,dst_term_list_file):
    worker = controller(src_dir_list,dst_doc_name_file,dst_term_list_file,3)
    worker.run()

def SingleThread(content_file,name_file):
    reader = IOWorker([name_file,content_file])
    worker = MapWorker()
    writer = IOWorker([name_file + '.single',content_file + '.single'],mode='w')
    start_time = time.clock()
    count = 0
    while 1:
        task_raw_data = reader.GetLines()
        data_count = len(task_raw_data)
        if data_count == 0:
            break
        task_raw_data = worker.run(task_raw_data)
        writer.SaveLines([Converter.MapResultToString(item) for item in task_raw_data])
        count+=data_count
        if count % 300 == 0:
            speed = 60 * count / (time.clock() - start_time)
            print '\r',u'完成%d,速度%d个/分钟' % (count,speed),
    writer.Close()
    speed = 60 * count / (time.clock() - start_time)
    print '\r',u'完成%d,速度%d个/分钟' % (count,speed),
    print u'\n任务全部执行完毕,输出到%s.single' % content_file

def MultiThread(content_file,name_file):
    manager = Manager(content_file,name_file,10)
   #manager.runInMultiProcess()
    manager.run()
    start_time = time.clock()
    while 1:
        speed = 60 * manager.finished / (time.clock() - start_time)
        print '\r',u'完成%d,速度%d个/分钟,%d,%d' % (manager.finished,speed,manager.task_queue.qsize(),manager.result_queue.qsize()),
        time.sleep(1)

def ReduceSingleThread(content_file):
    print u'创建倒排索引...'
    worker = ReduceWorker()
    reader = IOWorker([content_file],20000)
    doc_list = []
    count = 0
    save_count = 0
    start_time = time.clock()
    clear_dir('tmp')
    while 1:
         task_raw_data = reader.GetLines()
         data_count = len(task_raw_data)
         if data_count == 0:
            break
         doc_list = Converter.ToDocList(task_raw_data,count)
         count+=data_count
         term_inverter_list = worker.run(doc_list)
         term_inverter_string = Converter.TermInverterToString(term_inverter_list)
         save_count+=1
         IOWorker.SaveText('tmp\\%d' % save_count,term_inverter_string)
         print_speed(start_time,count)
    print u'保存完毕，共%d个临时文件' % save_count

def MergeThread(dir,merged_file):
    print u'合并倒排索引中，输出文件%s' % merged_file
    file_list = ['%s\\%s' % (dir,file) for file in os.listdir(dir)]
    merger = Merger(file_list,merged_file)
    merger.run()

#将倒排索引文件按词频分为两个文件
def DivideByFreqThread(term_inverter_file,threshold):
    print u'将倒排索引文件按词频分为两个文件'
    f_content = open(term_inverter_file)
    f_low = open(term_inverter_file + '.low','w')
    f_high = open(term_inverter_file + '.high','w')
    i = 0
    start_time = time.clock()
    tmp = True
    high_indexes = []
    while tmp:
        tmp = f_content.readline()
        if tmp == None or tmp == '':
            break
        index = tmp.index(',')
        index2 = tmp.index(',',index + 1)
        count = int(tmp[index + 1:index2])
        if count < threshold:
            f_low.write(tmp)
        else:
            high_indexes.append('%s:%s' % (tmp[:index],str(f_high.tell())))
            f_high.write(tmp)
        i+=1
        if i % 10000 == 0:
            print_speed(start_time,i)
    f_content.close()
    f_low.close()
    f_high.close()
    print_speed(start_time,i)
    print u'高低频词分离完毕,阈值：%d' % threshold
    print u'保存%s' % f_low.name
    print u'保存%s' % f_high.name
    write_all_text(f_high.name + '.index',','.join(high_indexes))
    print '保存%s.index' % f_high.name

#将正索引中词频低的词去掉
def RemoveLowWords(low_term_inverter_file,content_file):
    print u'将正索引中词频低的词去掉'
    file_low = open(low_term_inverter_file)
    low_term_list = []
    tmp = True
    while tmp:
        tmp = file_low.readline()
        if tmp == '' or tmp == None:
               break
        index = tmp.index(',')
        low_term_list.append(tmp[:index])
    file_low.close()

    low_term_set = set(low_term_list)
    print u'已载入低频词表'
    file_content = open(content_file)
    file_content_high = open(content_file + '.high','w')
    file_index_list = []
    tmp = True
    count = 0
    start_time = time.clock()
    while tmp:
        tmp = file_content.readline()
        if tmp == None or tmp == '':
            break
        term_list = ContentFile.ToTermList(tmp[:-1])
        term_list = ['%s:%s' % term for term in term_list if term[0] not in low_term_set]
        file_index_list.append(str(file_content_high.tell()))
        file_content_high.write(','.join(term_list))
        file_content_high.write('\n')
        count+=1
        if count % 10000 == 0:
            print_speed(start_time,count)
    file_content.close()
    file_content_high.close()
    print_speed(start_time,count)
    print u'已保存%s' % file_content_high.name
    file_content_high_index = open(content_file + '.high.index','w')
    file_content_high_index.write(','.join(file_index_list))
    file_content_high_index.close()
    print u'已保存%s' % file_content_high_index.name

def CreateFileLineIndex(content_high_file):
    file_content_high = open(content_high_file)
    file_index_list = []
    tmp = True
    count = 0
    while tmp:
        posi = file_content_high.tell()
        tmp = file_content_high.readline()
        if tmp == '' or tmp == None:
            break
        file_index_list.append(str(posi))
        count+=1
        if count % 10000 == 0:
            print count
    file_content_high.close()
    file_content_high_index = open(content_high_file + '.index','w')
    file_content_high_index.write(','.join(file_index_list))
    file_content_high_index.close()
    print u'已保存%s' % file_content_high_index.name

def CreateDocVector(term_inverter_file,content_list_file,doc_vector_file):
    print u'创建doc vectors...'
    term_dict = {}    #存储(termid,每个term对应的出现文档个数)
    tmp = True
    f_inverter = open(term_inverter_file)
    
    i = 0
    while tmp:
        tmp = f_inverter.readline()
        if tmp == None or tmp == '':
            break
        item = MergeReader.ToKeyValue(tmp)
        term_dict[item[0]] = (i,int(item[2]))
        i+=1
    f_inverter.close()
    print u'总词数:%d' % len(term_dict)

    f_doc_vector = open(doc_vector_file,'w')
    f_content = open(content_list_file)
    tmp = True
    start_time = time.clock()
    count = 0
    indexes = []
    while tmp:
        tmp = f_content.readline()
        if tmp == None or tmp == '':
            break
        count+=1
        term_list = Converter.ToTermList(tmp)
        #w_list为weight集合
        w_list = [CalWeight(int(term[1]),term_dict[term[0]][1],N) for term in term_list]
        #归一化
        w_list = Normalize(w_list)
        term_id_list = [term_dict[term[0]][0] for term in term_list]
        #w_list为(termid,weight)
        w_list = ['%d:%f' % (term_id_list[i],w_list[i]) for i in range(len(w_list))]
        indexes.append(str(f_doc_vector.tell()))
        f_doc_vector.write(','.join(w_list))
        f_doc_vector.write('\n')
        if count % 10000 == 0:
            print_speed(start_time,count)
    print_speed(start_time,count)
    f_content.close()
    f_doc_vector.close()
    print '保存%s' % f_doc_vector.name  
    write_all_text(doc_vector_file + '.index',','.join(indexes))
    print '保存%s.index' % doc_vector_file

def CalWeight(tftd,nt,N):
    return tftd * log(N / nt + 0.01)

def Normalize(w_list):
    s = sqrt(sum([w * w for w in w_list]))
    return [w / s for w in w_list]

def Search(*args):
    searcher = Searcher(*args)
    while 1:
        term = raw_input()
        searcher.search(term)

def PreDealTopics(topic_file,output_dir):
    worker = pre_worker(None,None,None)
    topic_list = worker.parse_topic(topic_file)
    topicNameFile=output_dir + ur'\topic_name.txt'
    topicContentFile=output_dir + ur'\topic_content.txt'
    write_all_text(topicNameFile,'\n'.join([topic[0] for topic in topic_list]))
    write_all_text(topicContentFile,'\n'.join([topic[1] for topic in topic_list]))
    print u'查询集处理完毕'
    return (topicNameFile,topicContentFile)

def LoadTopics(topic_name_f,topic_content_f):
    (topic_list,term_doc_count) = Topic.LoadTopicList(topic_name_f,topic_content_f)
    for topic in topic_list:
        w_list = Normalize([CalWeight(topic.term_dict[term],term_doc_count[term],N) for term in topic.term_dict])
        count = 0
        for term in topic.term_dict:
            topic.term_dict[term] = w_list[count]
            count+=1
    return topic_list

def CalDistance(topic,searcher):
    doc_result = {}
    print len(topic.term_dict)
    count = 0
    start_time = time.clock()
    doc_list = []
    for term in topic.term_dict:
        doc_list.extend(searcher.search(term))
        count+=1
    count = 0
    doc_list = set(doc_list)
    print len(doc_list)
    start_time = time.clock()
    for docid in doc_list:
        term_list = searcher.searchByDocId(docid)
        count+=1
        if count % 1000 == 0:
            print_speed(start_time,count)
    print len(doc_list)

#根据倒排索引计算文档距离，完全抛弃doc_content
def CalDistance2(topic,searcher):
    doc_result = {}
    count = 0
    start_time = time.clock()
    print 'topic',topic.id,u' 词数:',len(topic.term_dict)
    for term in topic.term_dict:
        doc_list = searcher.search(term)
        for doc in doc_list:
            doc_result[doc[0]] = doc_result.get(doc[0],0) + float(doc[1]) * topic.term_dict[term]
        count+=1
        print_speed(start_time,count)
    print_speed(start_time,count)
    print ''
    doc_result = sorted(doc_result.iteritems(), key=itemgetter(1), reverse=True)
    return doc_result[:1000]

#将doc_vector转换成二进制
def ToBinary(doc_vector_file):
    f_vector = open(doc_vector_file)
    f_bin = open(doc_vector_file + '.bin','wb')
    content = True
    indexes = []
    count = 0
    start_time = time.clock()
    while content:
        content = f_vector.readline()
        if content == '' or content == None:
            break
        if content.endswith('\n'):
            content = content[:-1]
        items = content.split(',')
        term_list = []
        for item in items:
            data = item.split(':')
            term_list.append((int(data[0]),float(data[1])))
        indexes.append(str(f_bin.tell()))
        f_bin.write(struct.pack('i',len(term_list)))
        for term in term_list:
            f_bin.write(struct.pack('i',term[0]))
            f_bin.write(struct.pack('f',term[1]))
        
        count+=1
        if count % 1000 == 0:
            print_speed(start_time,count)
    write_all_text(doc_vector_file + '.bin.index',','.join(indexes))
    f_bin.close()

#将doc_vector直接转换成倒排索引
def ConvertDocVectorToTermVector(doc_vector_file,term_vector_file):
    f_vector = open(term_vector_file,'w')
    f_index = open(term_vector_file + '.index','w')
    begin = 0
    step = 40000
    end = step
    max_term_id = 0
    term_dict = {}
    while 1:
        print 'begin=%d,end=%d,max_term_id=%d' % (begin,end,max_term_id)
        start_time = time.clock()
        doc_id = 0
        term_dict = {}
        #循环读一次正向索引
        f_doc = open(doc_vector_file,'rb')
        while 1:
            try:
                count = readInt(f_doc)
            except:
                break
            #读一个文档的正向索引
            for i in range(count):
                term_id = readInt(f_doc)
                if term_id > max_term_id:max_term_id = term_id
                weight = readFloat(f_doc)
                if term_id >= begin and term_id < end:
                    if term_dict.has_key(term_id):
                        term_dict[term_id].append((doc_id,weight))
                    else:
                        term_dict[term_id] = [(doc_id,weight)]
            doc_id+=1
            if doc_id % 1000 == 0:
                print_speed(start_time,doc_id)
            
        f_doc.close()
        if len(term_dict) > 0:
            term_list = term_dict.items()
            term_list.sort()
            for term in term_list:
                term_id = term[0]
                doc_list = ['%d:%f' % doc for doc in term[1]]
                f_index.write('%d:%ld,' % (term_id,f_vector.tell()))
                f_vector.write('%d,%s\n' % (term_id,','.join(doc_list)))
            del term_list
        del term_dict
        begin = end
        end = begin + step
        if begin > max_term_id:
            break
        gc.collect()
    f_vector.close()
    f_index.close()
    print u'已保存%s' % f_vector.name

def CreateTermName(dir,term_name_file):
    content = get_all_text(dir + '\\term_inverter_index_file.txt.high.index')
    names = [item.split(':')[0] for item in content.split(',') if item is not '']
    write_all_text(term_name_file,'\n'.join(names))

def WriteResult(doc_result,topic_id,f_result,searcher):
    count = 0
    run_id = 1
    #query_num Q0 document_num rank socre run_id
    for doc in doc_result:
        count+=1
        f_result.write('%d  Q0  %s  %d  %f  %d\n' % (topic_id,searcher.doc_names[int(doc[0])],count,doc[1],run_id))

def main():
    env = platform.system()
    print u'系统环境@',env
    INPUT_PATH = ur'f:\datasets\Robust04'
    OUTPUT_DIR = ur'f:\datasets\2016output'

    input_dir_list = [os.path.join(INPUT_PATH,'DISK_4'),os.path.join(INPUT_PATH,'DISK_5')]
    topic_file = os.path.join(INPUT_PATH,r'Topics\robust04.topics.txt')

    #该文件为所有term的集合，每一行表示一个doc的term，term按照原文顺序排布，可以用来进行word2vec训练
    raw_term_list_file = os.path.join(OUTPUT_DIR,'raw_term_list.txt')
    
    #和raw_term_list_file文件对应，每一行表示一个doc的名字
    doc_name_file = os.path.join(OUTPUT_DIR,'doc_name.txt')

    #term的倒排索引，格式为term,总频次,{docid,docid...}
    term_inverter_index_file = os.path.join(OUTPUT_DIR,'term_inverter_index_file.txt')

    doc_vector_file = ur'f:\datasets\2016output\doc_vector.txt'
    
    term_vector_file = ur'f:\datasets\2016output\term_vector.txt'
    term_name_file = ur'f:\datasets\2016output\term_name.txt'
    result_file = ur'f:\datasets\2016output\result.txt'
    start_time = time.clock()

    
    PreDealThread(input_dir_list,doc_name_file,raw_term_list_file)

    SingleThread(raw_term_list_file,doc_name_file)
    ReduceSingleThread(raw_term_list_file + '.single')
    
    MergeThread('tmp',term_inverter_index_file)
    DivideByFreqThread(term_inverter_index_file,5)
    RemoveLowWords(term_inverter_index_file + '.low',raw_term_list_file + '.single')

     
    CreateDocVector(term_inverter_index_file + '.high',raw_term_list_file + '.single.high',doc_vector_file)

    ToBinary(doc_vector_file)

    ConvertDocVectorToTermVector(doc_vector_file+".bin",term_vector_file)
    

    CreateTermName(OUTPUT_DIR,term_name_file)
    
    topic_files=PreDealTopics(topic_file,OUTPUT_DIR)
    
    topic_list = LoadTopics(topic_files[0],topic_files[1])
    searcher = Searcher2(term_vector_file,term_name_file)
    f_result = open(result_file,'w')
    searcher.loadDocNames(doc_name_file)
    for topic in topic_list:
        doc_result = CalDistance2(topic,searcher)
        WriteResult(doc_result,topic.id,f_result,searcher)
    f_result.close()
    print u'共耗时：%.2f分钟' % (1.0 * (time.clock() - start_time) / 60)


if __name__ == '__main__':
    main()
