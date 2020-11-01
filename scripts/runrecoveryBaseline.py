import platform
apps = []
for workload in ['YCSB']:
  for log_type in ['D']:
    apps.append("rundb_B%s_%s -Lr0" % (log_type, workload))
# app = "rundb_TC_TPCC -Lr1"
# print(apps)
if platform.system() == 'Darwin':
    apps = ['./rundb']
threads = [1, 2, 4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 48, 52, 56, 60, 64] # [1, 2, 6, 10, 20, 26, 30, 36, 40] # , 50, 60, 70, 76, 80] # [1, 5, 10, 15, 20, 25, 30, 35, 40] # [1, 5, 10, 15, 20, 25, 30, 35, 40] # [3, 5, 10, 20, 30, 40]
logs = [4] #  2, 4, 8, 16] # [1, 2, 4]
import subprocess
import re
resultRE = re.compile(r'Throughput:\s+([\d\.\+e\-]+)')
modification = 'recover_'
label = subprocess.check_output(["git", "describe"]).strip()
resDir = './results/' + modification + label.decode('ascii')
import os
if not os.path.exists(resDir):
    os.makedirs(resDir)
matlabCode = ""
trials = 4
resDict = {}
appResDict = {}
for app in apps:
  print(app)
  for log_num in logs:
    algP = []
    for t in threads:
      if t < log_num or t % log_num !=0:
         algP.append(0.)
         continue
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
      algP.append((t, avgThr))
    print("y_log%d = %s;" % (log_num, repr(algP)))
    matlabCode += "y_log%d = %s;\n" % (log_num, repr(algP))
    key = "log%d" % log_num
    resDict[key] = algP
    print("plot(x, y_log%d, '--', 'DisplayName', '%s');" % (log_num, "y-" + str(log_num)))
    matlabCode += "plot(x, y_log%d, '--', 'DisplayName', '%s', 'lineWidth', 5);" % (log_num, "y-" + str(log_num))
  print(resDict)
  appResDict[app] = resDict
print(appResDict)

