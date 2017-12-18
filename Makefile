N_HOSTS= 3
CMD= mpiexec -hostfile hostfile -n ${N_HOSTS}


demo:
	${CMD} demo.py --size 10 --verbose


test:
	${CMD} tests.py
