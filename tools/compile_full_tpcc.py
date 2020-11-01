import sys
import os
import sys
import re
import os.path
import platform
import subprocess


def replace(filename, pattern, replacement):
    f = open(filename)
    s = f.read()
    f.close()
    s = re.sub(pattern, replacement, s)
    f = open(filename, 'w')
    f.write(s)
    f.close()


dbms_cfg = ["config-std.h", "config.h"]
algs = ['no', 'serial', 'parallel']


def insert_his(alg, workload='YCSB', cc_alg='NO_WAIT', log_type='LOG_DATA', recovery='false',
               gc='false', ramdisc='true', max_txn=100000, withold_log='false'):
    if alg == 'no':
        name = 'N'
    else:
        name = 'S' if alg == 'serial' else 'P'
        if alg == 'serial':
            name = 'S'
        elif alg == 'parallel':
            name = 'P'
        elif alg == 'batch':
            name = 'B'
        elif alg == 'taurus':
            name = 'T'
        else:
            assert(False)

    if log_type == 'LOG_DATA':
        name += 'D'
    elif log_type == 'LOG_COMMAND':
        name += 'C'
    else:
        assert(False)
    
    name += '_%s_%s' % (workload, cc_alg.replace('_', ''))
    

    if withold_log == 'true':
        assert(alg=='serial' and log_type=='LOG_COMMAND')
        name = name.replace('SC', 'SX')
        jobs[name] = {}
        jobs[name]["WITHOLD_LOG"] = 'true'
    else:
        jobs[name] = {}

    
    jobs[name]['TPCC_FULL'] = 'true'
    jobs[name]['TPCC_DBX1000_SERIAL_DELIVERY'] = 'false'
    jobs[name]['TPCC_INSERT_INDEX'] = 'true'
    jobs[name]['TPCC_INSERT_ROWS'] = 'true'
    jobs[name]['TPCC_DELETE_ROWS'] = 'true'
    jobs[name]['TPCC_DELETE_INDEX'] = 'true'

    jobs[name]['TPCC_SPLIT_DELIVERY'] = 'false'
    jobs[name]['TPCC_VALIDATE_GAP'] = 'false'
    jobs[name]['TPCC_VALIDATE_NODE'] = 'false'
    jobs[name]['SIMPLE_INDEX_UPDATE'] = 'false'
    jobs[name]['MAX_SCAN_PER_TXN'] = '30'
    
    jobs[name]["LOG_ALGORITHM"] = "LOG_%s" % alg.upper()
    jobs[name]["WORKLOAD"] = workload.replace('TPCF', 'TPCC')
    jobs[name]["LOG_TYPE"] = log_type
    jobs[name]["CC_ALG"] = cc_alg
    jobs[name]["MAX_TXNS_PER_THREAD"] = '(150000)'
    jobs[name]['BIG_HASH_TABLE_MODE'] = '(true)'
    jobs[name]['PROCESS_DEPENDENCY_LOGGER'] = '(false)'
    jobs[name]["MAX_LOG_ENTRY_SIZE"] = '32768'
    if alg == 'no':
        # jobs[name]["USE_LOCKTABLE"] = 'false'
        jobs[name]["USE_LOCKTABLE"] = 'true'
        jobs[name]["LOCKTABLE_INIT_SLOTS"] = '(1)'
        jobs[name]["LOCKTABLE_MODIFIER"] = '(10003)'
    if alg == 'batch':
        jobs[name]["USE_LOCKTABLE"] = 'false'
        jobs[name]['MAX_NUM_EPOCH'] = '100000'
        # jobs[name]['RECOVERY_FULL_THR'] = 'true' # better measurement
    if alg == 'serial':
        jobs[name]["COMPRESS_LSN_LOG"] = 'false'
        jobs[name]["USE_LOCKTABLE"] = 'true'
        jobs[name]["LOG_BUFFER_SIZE"] = '26214400'

        
    if alg == 'taurus':
        jobs[name]['LOCKTABLE_INIT_SLOTS'] = '(1)'
        # jobs[name]['BIG_HASH_TABLE_MODE'] = '(false)'
        if True:
            # jobs[name]['ASYNC_IO'] = 'false' # try
            jobs[name]["LOCKTABLE_MODIFIER"] = '(10003)'
            if log_type == 'LOG_DATA':
                jobs[name]['PROCESS_DEPENDENCY_LOGGER'] = '(false)'
    jobs[name]["LOG_FLUSH_INTERVAL"] = '0' # we do not want the flushing time to disturb the result.
    #if cc_alg == 'NO_WAIT' and log_type == 'LOG_COMMAND':
    #    jobs[name]["UPDATE_SIMD"] = '(true)' # experimental
    #else:
    #    jobs[name]["UPDATE_SIMD"] = '(false)'
        
    if cc_alg == 'SILO':
        jobs[name]["SCAN_WINDOW"] = '2'
        jobs[name]["PER_WORKER_RECOVERY"] = '(false)'
        # jobs[name]["RECOVER_SINGLE_RECOVERLV"] = '(true)'
    else:
        jobs[name]["SCAN_WINDOW"] = '2'
        jobs[name]["PER_WORKER_RECOVERY"] = '(false)'
        # jobs[name]["RECOVER_SINGLE_RECOVERLV"] = '(false)'

    #jobs[name]["LOG_RECOVER"] = recovery
    #jobs[name]["LOG_GARBAGE_COLLECT"] = gc
    #jobs[name]["LOG_RAM_DISC"] = ramdisc
    #jobs[name]["MAX_TXN_PER_THREAD"] = max_txn


jobs = {}
# benchmarks = ['YCSB']
# benchmarks = ['TPCC']
if len(sys.argv) > 1:
    insert_his(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
else:
    benchmarks = ['TPCF']
    for bench in benchmarks:
        #insert_his('parallel', bench, 'LOG_DATA')
        #insert_his('parallel', bench, 'LOG_COMMAND')
        #insert_his('batch', bench, 'LOG_DATA')
        
        insert_his('no', bench, 'NO_WAIT', 'LOG_DATA')
        insert_his('serial', bench, 'NO_WAIT', 'LOG_DATA')
        insert_his('serial', bench, 'NO_WAIT', 'LOG_COMMAND')
        insert_his('serial', bench, 'NO_WAIT', 'LOG_COMMAND', withold_log='true')
        
        insert_his('taurus', bench, 'NO_WAIT', 'LOG_COMMAND')
        insert_his('taurus', bench, 'NO_WAIT', 'LOG_DATA')

        

for (jobname, v) in jobs.items():
    os.system("cp " + dbms_cfg[0] + ' ' + dbms_cfg[1])
    for (param, value) in v.items():
        pattern = r"\#define\s*" + re.escape(param) + r'.*'
        replacement = "#define " + param + ' ' + str(value)
        replace(dbms_cfg[1], pattern, replacement)

    command = "make clean; make -j32; cp rundb rundb_%s" % (jobname)
    print("start to compile " + jobname)
    proc = subprocess.Popen(command, shell=True,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    while proc.poll() is None:
        # print proc.stdout.readline()
        commandResult = proc.wait()  # catch return code
        # print commandResult
        if commandResult != 0:
            print("Error in job. " + jobname)
            print(proc.stdout.read())
            # print proc.stderr.read()
            print("Please run 'make' to debug.")
            exit(0)
        else:
            print(jobname + " compile done!")
