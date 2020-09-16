import unittest
import ijr.firefly as ff
import os


class MyTestCase(unittest.TestCase):
    def test_something(self):
        x = ff.Secrets()
        y = x.dot_secret(os.environ['MONGO'])
        print(y)


if __name__ == '__main__':
    unittest.main()
