import platform
import sys
workload = 'YCSB'
log_type = 'C'
log_buffer = 52428800
if workload == 'TPCC' and log_type == 'D':
  log_buffer *= 10
app = "rundb_T%s_%s" % (log_type, workload)
if platform.system() == 'Darwin':
    app = './rundb'
threads = [1, 2, 5, 10, 15, 20, 25, 30, 36, 40, 42, 48, 52, 56, 60, 64] # , 36, 38, 40] # , 50, 60, 70, 76, 80] # [1, 5, 10, 15, 20, 25, 30, 35, 40] # [1, 5, 10, 15, 20, 25, 30, 35, 40] # [3, 5, 10, 20, 30, 40]
logs = [1, 2, 4] # [1, 2, 4] #  2, 4, 8, 16] # [1, 2, 4]
import subprocess
import re
resultRE = re.compile(r'Throughput:\s+([\d\.\+e\-]+)')
modification = 'scala_'
label = subprocess.check_output(["git", "describe"]).strip()
resDir = './results/' + modification + label.decode('ascii')
import os
if not os.path.exists(resDir):
    os.makedirs(resDir)
matlabCode = ""
trials = 4
resDict = {}
for k in range(trials):
  for log_num in logs:
    for t in threads:
      key = '%d_%d' % (log_num, t)
      if not key in resDict:
        resDict[key] = []
      
      while True:
          try:
            subprocess.check_output("rm -f core", shell=True)
            ret = subprocess.check_output("numactl -i all -- ./%s -Ln%d -t%d -Lb%d" % (app, log_num, t, log_buffer), shell=True).decode('ascii')
            # ret = subprocess.check_output("./%s -Ln%d -t%d -Lb524288000" % (app, log_num, t), shell=True).decode('ascii') # to get the core dump
            # print(ret)
            open(resDir + '/%s_ln%d_th%d_%d.txt' % (app, log_num, t, k), 'w').write(ret)
            # print ret
            thr = float(resultRE.findall(ret)[0])
            
            break
          except Exception as e:
            # subprocess.check_output("core", shell=True)
            print(e, "Core dumped.")
            sys.exit(0)
      resDict[key].append(thr)
      #print(t, tmpList)
ptrResult = {}
for log_num in logs:
    algP = []
    for t in threads:
      key = '%d_%d' % (log_num, t)
      avgThr = sum(resDict[key]) / float(trials)
      algP.append(avgThr)
    print("y_log%d = %s;" % (log_num, repr(algP)))
    matlabCode += "y_log%d = %s;\n" % (log_num, repr(algP))
    key = "log%d" % log_num
    ptrResult[key] = algP
    print("plot(x, y_log%d, '--', 'DisplayName', '%s');" % (log_num, "y-" + str(log_num)))
    matlabCode += "plot(x, y_log%d, '--', 'DisplayName', '%s', 'lineWidth', 5);" % (log_num, "y-" + str(log_num))
print(ptrResult)
