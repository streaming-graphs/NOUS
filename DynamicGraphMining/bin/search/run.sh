# note a yarn executor is an instance of JVM. Each physical compute node can have multiple yarn-executors. Each executor can consists of multiple cores(parallel tasks that share resources in memory). An adavnatage of diving each node in multiple executor(JVM) is if an executor fails , other executor on same node can take the tasks up. Also, too many cores(tasks) in executor can slow it down due to 
nexe=15
plen=4
deg=10000
spark-submit --master yarn \
    --deploy-mode client \
    --driver-memory 32g \
    --executor-memory 32g \
    --num-executors $nexe \
    --executor-cores 5 \
	--class gov.pnnl.aristotle.algorithms.PathSearch.PathSearchPregel \
	/people/d3x771/projects/knowledgeGraph/uber-knowledge_graph-0.1-SNAPSHOT.jar \
   	yago2/tmp/yago*.ttl \
	PathSearchBenchmark/entityPairsPerPer.txt \
	/people/d3x771/projects/knowledgeGraph/PathSearchBenchmark/timingBenchmarks/len$plen\_exe$nexe/ \
	$plen $deg
