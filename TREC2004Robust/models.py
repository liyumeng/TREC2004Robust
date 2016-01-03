from utils import get_all_text
class ContentFile(object):
    def __init__(self):
        pass
    @staticmethod
    def ToTermList(line):
        term_list = line.split(',')
        term_list = [tuple(term.split(':')) for term in term_list if term is not '']
        return term_list

class Topic(object):
    def __init__(self):
        pass
    @staticmethod
    def LoadTopicList(topic_name_file,topic_content_file):
        ids = get_all_text(topic_name_file).split('\n')
        content_list = get_all_text(topic_content_file).split('\n')
        topic_list = []
        term_doc_count = {}
        for i in range(len(ids)):
            if ids[i] == '':
                continue
            t = Topic()
            t.id = int(ids[i])
            t.term_dict = {}
            for term in content_list[i].split(' '):
                if t.term_dict.has_key(term):
                    t.term_dict[term]+=1
                else:
                    t.term_dict[term] = 1
                    term_doc_count[term] = term_doc_count.get(term,0) + 1
            topic_list.append(t)
        return (topic_list,term_doc_count)