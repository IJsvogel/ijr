import unittest
import ijr.firefly as ff
import os
import warnings



class TestFF(unittest.TestCase):

    def test_(self):
        warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)
        x = ff.Secrets()
        y = x.dict_secret(os.environ['MONGO'])
        self.assertIsInstance(y, dict)


if __name__ == '__main__':
    unittest.main()
