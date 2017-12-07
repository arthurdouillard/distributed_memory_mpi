N_HOSTS= 3
CMD= mpiexec -hostfile hostfile -n ${N_HOSTS}
LOG= critical


demo:
	${CMD} demo.py --log ${LOG}


test:
	${CMD} tests.py
