import platform
workload = 'TPCC'
app = "rundb_SD_%s -Lr1" % workload
if platform.system() == 'Darwin':
    app = './rundb'
threads = [1, 2, 5, 10, 15, 20, 25, 30, 36, 40, 42, 48, 52, 56, 60, 64]# [1, 2, 5, 10, 15, 20, 25, 30, 36, 38, 40] # [2, 5, 10, 20, 25, 30, 36, 40, 50, 60, 70, 76, 80] # [1, 5, 10, 15, 20, 25, 30, 35, 40] # [1, 5, 10, 15, 20, 25, 30, 35, 40] # [3, 5, 10, 20, 30, 40]
logs = [1] # [1, 2, 4]
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
for log_num in logs:
    algP = []
    for t in threads:
      tmpList = []
      for k in range(trials):
        ret = subprocess.check_output("numactl -i all -- ./%s -Ln%d -t%d" % (app, log_num, t), shell=True).decode('ascii')
        # print(ret)
        open(resDir + '/%s_ln%d_th%d_%d.txt' % (app, log_num, t, k), 'w').write(ret)
        # print ret
        thr = float(resultRE.findall(ret)[0])
        tmpList.append(thr)
      print(t, tmpList)
      avgThr = sum(tmpList) / float(trials)
      algP.append(avgThr)
    print("y_log%d = %s;" % (log_num, repr(algP)))
    matlabCode += "y_log%d = %s;\n" % (log_num, repr(algP))
    key = "logno" #  % log_num
    resDict[key] = algP
    print("plot(x, y_log%d, '--', 'DisplayName', '%s');" % (log_num, "y-" + str(log_num)))
    matlabCode += "plot(x, y_log%d, '--', 'DisplayName', '%s', 'lineWidth', 5);" % (log_num, "y-" + str(log_num))
print(resDict)
