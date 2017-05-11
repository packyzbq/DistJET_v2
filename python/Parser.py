import re
import os

class Parser:
    fatalPattern = [
        # C
        '.*Segmentation fault',
        # SNiPER
        '.*ERROR:',
        '.*FATAL:',
        # Python
        '.*IOError',
        '.*ImportError',
        '.*TypeError',
        '.*MemoryError',
        '.*SyntaxError',
        '.*NameError',
        '.*RuntimeError',
        # Other
        '.*\*\*\* Break \*\*\* segmentation violation',
        '.*Warning in <TApplication::GetOptions>: macro .* not found',
    ]

    successPattern = []

    def __init__(self,cfg):
        self.fatal = Parser.fatalPattern
        self.success = Parser.successPattern

    def parse(self, word):
        if not word:
            return True, None
        wordl = word.split('\n')
        for pattern in self.fatal:
            for w in wordl:
                match = pattern.match(word)
                if match:
                    return False, "Fatal line: %s"%w
        return True,None
