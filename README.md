# TREC2004Robust
采用TREC 2004 Robust Retrieval Track语料集，完成检索任务。

主要包括以下处理流程：
### 语料预处理
包括分词、词干化、去停用词等。
使用`PreDealThread(inputs,docname_output,terms_output)`进行预处理，其中`inputs`表示输入的文件夹列表，文档的名称输出到`docname_output`文件中，文档的内容经过处理后输出到`terms_output`中。

共2293个文件，使用3个进程处理，速度约为57个/分钟。

### 建立倒排索引
对整个文档集合建立倒排索引（Inverted Index），以加快后续的检索速度。同时对训练集合中的每个文档，建立对应的VSM向量（VSM可以看作是文档的正向索引，Forward Index）
