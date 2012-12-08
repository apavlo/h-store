import os, sys

prunelist = ('hsqldb19b3', 
             'hsqldb', 
             'proj_gen',
             'jni_md.h',
             'jni.h',
             'org_voltdb_jni_ExecutionEngine.h',
             'org_voltdb_utils_DBBPool.h',
             'org_voltdb_utils_DBBPool_DBBContainer.h',
             'org_voltdb_utils_ThreadUtils.h',
             'xml2',
             'simplejson')

def readFile(filename):
    "read a file into a string"
    FH=open(filename, 'r')
    fileString = FH.read()
    FH.close()
    return fileString

def processFile(f, approvedLicensesJavaC, approvedLicensesPython):
    if not f.endswith(('.java', '.cpp', '.cc', '.h', '.hpp', '.py')):
        return 0
    content = readFile(f)
    if f.endswith('.py'):
        if not content.startswith("#"):
            if content.lstrip().startswith("#"):
                print "ERROR: \"%s\" contains whitespace before initial comment." % f
            else:
                print "ERROR: \"%s\" does not begin with a comment." % f
        elif not content.startswith(approvedLicensesPython):
            print "ERROR: \"%s\" does not start with an approved license." % f
        else:
            return 0
    else:
        if not content.startswith("/*"):
            if content.lstrip().startswith("/*"):
                print "ERROR: \"%s\" contains whitespace before initial comment." % f
            else:
                print "ERROR: \"%s\" does not begin with a comment." % f
        elif not content.startswith(approvedLicensesJavaC):
            print "ERROR: \"%s\" does not start with an approved license." % f
        else:
            return 0
    return 1

def processAllFiles(d, approvedLicensesJavaC, approvedLicensesPython):
    files = os.listdir(d)
    errcount = 0
    for f in [f for f in files if not f.startswith('.') and f not in prunelist]:    
        fullpath = os.path.join(d,f)
        if os.path.isdir(fullpath):
            errcount += processAllFiles(fullpath, approvedLicensesJavaC, approvedLicensesPython)
        else:
            errcount += processFile(fullpath, approvedLicensesJavaC, approvedLicensesPython)
    return errcount

testLicenses =   ['approved_licenses/mit_x11_hstore_and_voltdb.txt',
                  'approved_licenses/mit_x11_evanjones_and_voltdb.txt',
                  'approved_licenses/mit_x11_michaelmccanna_and_voltdb.txt',
                  'approved_licenses/mit_x11_voltdb.txt']
srcLicenses =    ['approved_licenses/gpl3_hstore_and_voltdb.txt',
                  'approved_licenses/gpl3_evanjones_and_voltdb.txt',
                  'approved_licenses/gpl3_nanohttpd_and_voltdb.txt',
                  'approved_licenses/gpl3_voltdb.txt']
testLicensesPy = ['approved_licenses/mit_x11_voltdb_python.txt']
srcLicensesPy =  ['approved_licenses/gpl3_voltdb_python.txt']

errcount = 0
errcount += processAllFiles("../src", 
                            tuple([readFile(f) for f in srcLicenses]),
                            tuple([readFile(f) for f in srcLicensesPy]))

errcount += processAllFiles("../tests", 
                            tuple([readFile(f) for f in testLicenses]),
                            tuple([readFile(f) for f in testLicensesPy]))
                            
print "Found %d missing licenses or incorrectly licensed files." % errcount
sys.exit(errcount)