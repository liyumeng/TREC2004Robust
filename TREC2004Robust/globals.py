

class setting(object):
    c_reg_doc = r'<DOC>(?P<content>[\s\S]+?)</DOC>'
    c_reg_doc_name = r'<DOCNO>(?P<content>[\s\S]+?)</DOCNO>'
    c_reg_doc_text = r'<TEXT>(?P<content>[\s\S]+?)</TEXT>'
    c_valid_chars = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
    c_min_length = 3    #设置单词的最小长度
    c_reg_topic = r'<top>(?P<content>[\s\S]+?)</top>'
    c_reg_topic_id = r'<num> Number: (?P<content>\d+)'
    c_reg_sub = r'<[\s\S]+?>'