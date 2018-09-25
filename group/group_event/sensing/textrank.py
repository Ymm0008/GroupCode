#-*- encoding:utf-8 -*-
import codecs
import sys
import json

from textrank4zh import TextRank4Keyword, TextRank4Sentence

reload(sys)
sys.setdefaultencoding('utf-8')


def text_rank(text, keywords_num):
	keywords = []
	tr4w = TextRank4Keyword()
	tr4w.analyze(text=text.encode('utf-8'), lower=True, window=2)   # py2中text必须是utf8编码的str或者unicode对象，py3中必须是utf8编码的bytes或者str对象
	for item in tr4w.get_keywords(keywords_num, word_min_len= 1):
		keywords.append(item.word)

	return json.dumps(keywords, ensure_ascii=False)


if __name__ == '__main__':

	text = "这间酒店位于北京东三环，里面摆放很多雕塑，文艺气息十足。答谢宴于晚上8点开始。"
	keywords=text_rank(text,5)
	print keywords
