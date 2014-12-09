from os.path import dirname, realpath, join
import sys


ROOT_PATH = dirname(dirname(realpath(__file__)))

sys.path.insert(0, join(ROOT_PATH, 'src'))


from happy.core import producer, consumer, HappyApp


@producer
def test_producer():
    words = 'this is some kind of sentence'
    for i in words.split():
        yield i


@consumer(producer=test_producer)
def word_count(item):
    pass


# print dir(test_producer)
# print word_count
# 
# test_producer.filter(lambda x: x % 2 == 0)
# test_producer.map(lambda x: 0)

app = HappyApp()
app.run()
