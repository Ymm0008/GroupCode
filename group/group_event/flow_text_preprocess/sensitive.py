#-*- encoding:utf-8 -*-
import codecs
import sys
import json
import os

reload(sys)
sys.setdefaultencoding('utf-8')

ab_path = os.path.join(os.path.dirname(__file__), 'dict')

sensitive_score_dict = { "1": 1,"2": 5,"3": 10}
sensitive_words_weight = dict()
for b in open(os.path.join(ab_path,'sensitive_words.txt'), 'rb'):
    word = b.strip().split('\t')[0]
    weight =  b.strip().split('\t')[1]
    sensitive_words_weight[word] =  weight

def createWordTree():
    wordTree = [None for x in range(256)]
    wordTree.append(0)
    nodeTree = [wordTree, 0]
    awords = []

    for b in open(os.path.join(ab_path,'sensitive_words.txt'), 'rb'):
        awords.append(b.strip().split('\t')[0])

    for word in awords:
        temp = wordTree
        for a in range(0,len(word)):
            index = ord(word[a])
            if a < (len(word) - 1):
                if temp[index] == None:
                    node = [[None for x in range(256)],0]
                    temp[index] = node
                elif temp[index] == 1:
                    node = [[None for x in range(256)],1]
                    temp[index] = node
                
                temp = temp[index][0]
            else:
                temp[index] = 1

    return nodeTree 


def searchWord(str, nodeTree):
    temp = nodeTree
    words = []
    word = []
    a = 0
    while a < len(str):
        index = ord(str[a])
        temp = temp[0][index]
        if temp == None:
            temp = nodeTree
            a = a - len(word)
            word = []
        elif temp == 1 or temp[1] == 1:
            word.append(index)
            words.append(word)
            a = a - len(word) + 1 
            word = []
            temp = nodeTree
        else:
            word.append(index)
        a = a + 1
    
    map_words = {}
    for w in words:
        iter_word = "".join([chr(x) for x in w])
        if not map_words.__contains__(iter_word):
            map_words[iter_word] = 1
        else:
            map_words[iter_word] = map_words[iter_word] + 1
    
    return map_words


def cal_sensitive(text):

    node = createWordTree()
    sensitive_words_dict = searchWord(text.encode('utf-8', 'ignore'), node)

    item = dict()
    if sensitive_words_dict:
        item['sensitive_words_string'] = "&".join(sensitive_words_dict.keys())
        item['sensitive_words_dict'] = json.dumps(sensitive_words_dict)
    else:
        item['sensitive_words_string'] = ""
        item['sensitive_words_dict'] = json.dumps({})

    score = 0
    if sensitive_words_dict:
        for k,v in sensitive_words_dict.iteritems():
            tmp_stage = sensitive_words_weight.get(k, 0)
            if tmp_stage:
                score += v*sensitive_score_dict[str(tmp_stage)]

    return score, item


if __name__ == '__main__':
    text = '暴政屠杀宝宝wink做的越来越好了[酷]'
    a,b = cal_sensitive(text)
    print a,b
