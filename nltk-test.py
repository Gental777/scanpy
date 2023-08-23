import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.probability import FreqDist

# 下载停用词列表
nltk.download('punkt')
nltk.download('stopwords')


# 输入文本
text = "Remove Any Cached Files: If Python has already imported your nltk.py file before you renamed it, there might be cached files causing the issue. Delete any .pyc files associated with your script. You can find these files in the same directory as your script."

# 分词
words = word_tokenize(text,'english')

# 去除停用词
stop_words = set(stopwords.words('english'))  # 使用英文停用词列表
filtered_words = [word for word in words if word.lower() not in stop_words]

# 统计词频1
fdist = FreqDist(filtered_words)

# 提取词频最高的前n个词作为关键词
top_keywords = fdist.most_common(5)  # 提取前5个关键词
print(top_keywords)
