# -*- coding: utf-8 -*-

from unittest import *
from data.datapaths import *

from raster2dggs.h3 import h3
from raster2dggs.rHP import rhp
from raster2dggs.geohash import geohash
from raster2dggs.maidenhead import maidenhead
from raster2dggs.s2 import s2


class TestRunthrough(TestCase):
    '''
    Parent class for the smoke tests. Handles temporary output files by
    overriding the built in setup and teardown methods from TestCase. Provides
    a new member function to recurse through nested output folders to empty
    them.
    '''
    
    def setUp(self):
        TEST_OUTPUT_PATH.mkdir(exist_ok=True)
        
    def tearDown(self):
        if TEST_OUTPUT_PATH.exists():
            self.clearOutFolder(TEST_OUTPUT_PATH)
            TEST_OUTPUT_PATH.rmdir()

    def clearOutFolder(self, folder):
        for child in folder.iterdir():
            if child.is_dir():
                self.clearOutFolder(child)
            else:
                child.unlink()

class TestH3(TestRunthrough):
    '''
    Test case description here
    '''
    
    
    def test_h3_run(self):
        try:
            #TODO: run h3 with standard test data
            #TODO first: raise artificial exception to develop except block
            print("Called TestH3.test_h3_run")
        
        except Exception as e:
            msg = "H3 runthrough failed with Exception.\n" #TODO: assemble message
            
            self.fail(msg)
    

class TestRHP(TestRunthrough):
    '''
    Test case description here
    '''
    
    def test_rhp_run(self):
        try:
            #TODO: run rhp with standard test data
            #TODO first: raise artificial exception to develop except block
            print("Called TestRHP.test_rhp_run")
        
        except Exception as e:
            msg = "rHP runthrough failed with Exception.\n" #TODO: assemble message
            
            self.fail(msg)
    

class TestGeohash(TestRunthrough):
    '''
    Test case description here
    '''
    
    def test_geohash_run(self):
        try:
            #TODO: run geohash with standard test data
            #TODO first: raise artificial exception to develop except block
            print("Called TestGeohash.test_geohash_run")
        
        except Exception as e:
            msg = "Geohash runthrough failed with Exception.\n" #TODO: assemble message
            
            self.fail(msg)
    

class TestMaidenhead(TestRunthrough):
    '''
    Test case description here
    '''
    
    def test_maidenhead_run(self):
        try:
            #TODO: run maidenhead with standard test data
            #TODO first: raise artificial exception to develop except block
            print("Called TestMaidenhead.test_maidenhead_run")
        
        except Exception as e:
            msg = "Maidenhead runthrough failed with Exception.\n" #TODO: assemble message
            
            self.fail(msg)
    

class TestS2(TestRunthrough):
    '''
    Test case description here
    '''
    
    def test_s2_run(self):
        try:
            #TODO: run s2 with standard test data
            #TODO first: raise artificial exception to develop except block
            print("Called TestS2.test_s2_run")
        
        except Exception as e:
            msg = "S2 runthrough failed with Exception.\n" #TODO: assemble message
            
            self.fail(msg)
