#!/usr/bin/python

import errno
import os
import shutil
import subprocess
import tempfile
import unittest

import valleak


class TempDir(object):
    def __init__(self):
        self.name = tempfile.mkdtemp()

    def __del__(self):
        shutil.rmtree(self.name)


def compileC(tempdir_path, text):
    exe_name = tempdir_path + "/test"
    source_name = exe_name + ".c"
    out = open(source_name, "w")
    out.write(text)
    out.close()
    gcc = subprocess.Popen(
            ("gcc", "-Wall", "-Werror", "-o", exe_name, source_name),
            stdout = subprocess.PIPE)
    error = gcc.wait()
    os.unlink(source_name)

    if error != 0:
        raise Exception("compile failed")

    return exe_name


class ValLeakTest(unittest.TestCase):
    def setUp(self):
        self.tempdir = TempDir()

    def valleak(self):
        return valleak.valleak(self.tempdir.name + "/test")

    def testNoError(self):
        compileC(self.tempdir.name, """
#include <stdio.h>

int main() {
  printf("hello world\\n");
  fprintf(stderr, "hello stderr\\n");
  return 0;
}
""")
        error, stdout, stderr = self.valleak()
        self.assertEquals(0, error)
        self.assertEquals("hello world\n", stdout)
        self.assertEquals("hello stderr\n", stderr)

    def testSimpleError(self):
        compileC(self.tempdir.name, """
#include <stdio.h>

int main() {
  printf("hello world\\n");
  fprintf(stderr, "hello stderr\\n");
  return 5;
}
""")
        error, stdout, stderr = self.valleak()
        self.assertEquals(5, error)
        self.assertEquals("hello world\n", stdout)
        # Note: no valgrind output
        self.assertEquals("hello stderr\n", stderr)

    def testSimpleValgrindError(self):
        compileC(self.tempdir.name, """
#include <stdio.h>
#include <stdlib.h>

int main() {
  int* array = (int*) malloc(sizeof(*array) * 2);
  printf("hello world %d\\n", array[2]);
  free(array);
  return 0;
}
""")
        error, stdout, stderr = self.valleak()
        self.assertEquals(1, error)
        assert stdout.startswith("hello world")
        # valgrind output
        assert "== ERROR SUMMARY" in stderr

    def testValgrindLeak(self):
        compileC(self.tempdir.name, """
#include <stdio.h>
#include <stdlib.h>

int main() {
  int* array = (int*) malloc(sizeof(*array) * 2);
  return 0;
}
""")
        error, stdout, stderr = self.valleak()
        self.assertEquals(1, error)
        self.assertEquals("", stdout)
        # valgrind output
        assert "== ERROR SUMMARY" in stderr


if __name__ == "__main__":
    # Mac OS X does not support valgrind, so this only runs the tests if supported
    try:
        valleak.valgrindVersion()
        unittest.main()
    except OSError, e:
        assert e.errno == errno.ENOENT
