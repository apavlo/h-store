# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2013 by H-Store Project
# Brown University
# Massachusetts Institute of Technology
# Yale University
# 
# http://hstore.cs.brown.edu/ 
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
# -----------------------------------------------------------------------
from __future__ import with_statement

## =====================================================================
## AbstractInstance
## =====================================================================
class AbstractInstance(object):
    def __init__(self, name):
        self.name = name
        self.id = None
        self.state = None
        self.public_dns_name = None
        self.private_dns_name = None
        self.image_id = None
        self.tags = { }
    ## DEF
    
    def __str__(self):
        return self.name
    def __unicode__(self):
        return self.name
    
    def start():
        pass
    
    def update():
        pass
    
    def stop():
        pass
    
    def terminate():
        pass
    
    def modify_attribute(self, key, value):
        pass
    
    def get_attribute(self, key):
        pass
    
## CLASS