dir = ur'd:\新生训练\pre_dealer'
file_index = open(dir + '\\content_list.txt.single.high.index')
file_content = open(dir + '\\content_list.txt.single.high')
indexex = file_index.readline()
file_index.close()
index_list = [long(index) for index in indexex[:-1].split(',') if index is not '']
print len(index_list)
while 1:
    term_list = [397219,398063,403060,405378,411125,411256,41847,424166,424930,426350,432414,43262,449473,457515,460077,473541,480218,48438,510566,51609,5239,17428]
    
    for t in term_list:
        file_content.seek(index_list[t])
        c = file_content.readline()
        print c
        raw_input()
        if 'zzzz:' not in c:
            print str(t) + ' n '
        else:
            print str(t) + ' y'
    print u'all'
