app = "numactl -i 0,1,2,3 -N 0,1,2,3 -- ./apps/independentset/independentset"
threads = [1, 5, 10, 20, 30, 40] # [1, 5, 10, 15, 20, 25, 30, 35, 40] # [1, 5, 10, 15, 20, 25, 30, 35, 40] # [3, 5, 10, 20, 30, 40]
batch = [1000] # [0, 1, 100, 200, 500, 1000, 2000, 5000, 10000000]
import subprocess
algs = ["detBase"] # ["nondet", "serial", "pull", "detBase", "detPrefix", "detDisjoint"] # , "orderedBase"]
inputFs = ["randLocalGraph_J_5_100000000", "rMatGraph_J_5_100000000", "3Dgrid_J_100000000"]
matlabCode = ""
trials = 1
resDict = {}
for inputF in inputFs:
  for alg in algs:
    algP = []
    # for b in batch:
    for t in threads:
      tmpList = []
      # t = 40
      b = 1000
      for k in range(trials):
        ret = subprocess.check_output("%s%d -%s -t=%d %s" % (app, b, alg, t, inputF), shell=True) 
        # print ret
        timeCost = ret.split('STAT,(NULL),Time,')[1].split(',')[2].split('\n')[0]
        tc = float(timeCost) / 1000
        thr = 1. / tc
        tmpList.append(thr)
      avgThr = sum(tmpList) / float(trials)
      algP.append(avgThr)
    print "y_galois_%s = %s;" % (alg + "_" + inputF, repr(algP))
    key = "galois_%s_%s" % (alg, inputF)
    resDict[key] = algP
    print "plot(x, y_galois_%s, '--', 'DisplayName', '%s');" % (alg, "galois-" + alg)
print resDict

