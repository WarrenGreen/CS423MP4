#I have imported everything that I believe we will need for the project
import psutil #I have never used this library before, make sure you pip install it (i have already done this for our regular vm, i do not know the password for the -s vm)
import threading #make sure to set all spawned threads to daemon threads, otherwise everything locks up on you
import socket #easymode socket library
import argparse #this library just grabs command line arguments, its extremely powerful but has confusing documentation
import pickle #use this library for transfering jobs -- sock.send(pickle.dumps(data)), pickle.loads(sock.recv(num_bytes))
import Queue #if you dequeue an empty queue this will block unless you set the parameter right, be careful
from transfer import TransferManager
import sys

#global throttle variable to be accessed by any thread
throttle = 1.0
job_queue = Queue.Queue()

# vars that are (probably) not thread safe
my_transfer = None
stopping = False

class Job:
	def __init__(self, job_id, data_slice):
		self.job_id = job_id
		self.data = data_slice

#make sure to install psutil before running
def main():
	global my_transfer, stopping

	parser = argparse.ArgumentParser()
	parser.add_argument("host")
	parser.add_argument("port")
	parser.add_argument('--node', choices=['remote', 'local'], nargs=1, type=str, required=True)

	#node now contains the string value 'remote' or 'local'
	args = parser.parse_args()
	node = args.node[0]
	host = args.host
	port = int(args.port)

	# start the thread ahead of time so it can begin processing jobs as soon as they
	# are available
	worker = threading.Thread(target=worker_thread)
	worker.daemon = True
	worker.start()

	if node == 'remote':
		my_transfer = TransferManager(host, port, slave=True)

		# read_jobs is a generator
		# so jobs get thrown on the queue the minute the client sees them
		for job in my_transfer.read_jobs():
			job_queue.put(job)

	else:
		my_transfer = TransferManager(host, port)
		bootstrap_phase()

	stopping = True
	worker.join()

	if node == 'remote':
		my_transfer.shutdown()

# assumes only called from the local node
def bootstrap_phase():
	jobs = []

	total_size = 1024*1024*32
	num_jobs = 512
	elements_per_job = total_size / num_jobs

	for i in range(num_jobs):
		job_data = [1.111111 for j in xrange(elements_per_job)]
		jobs.append(Job(i, job_data))

	# divide number of jobs in half
	my_half    = jobs[:num_jobs/2]
	other_half = jobs[num_jobs/2:]

	# throw each job in my half on the queue
	for job in my_half:
		job_queue.put(job)

	# transfer half the jobs to the remote node
	my_transfer.write_array_of_jobs(other_half)

def processing_phase():
	#launch worker thread
	#launch load balancer
	pass
def aggregation_phase():
	#transfer all results from remote to local node
	pass

def worker_thread():
	global stopping
	jobs_seen = 0

	job = job_queue.get()

	while not stopping:
		if job != None:
			jobs_seen += 1
			print ('\r%d' % job.job_id),
			sys.stdout.flush()

		try:
			job = job_queue.get(timeout=5)
		except Queue.Empty:
			job = None

	print
	print 'Saw %d jobs' % jobs_seen

def state_manager():
	pass
def adaptor():
	pass
def hardware_monitor(throttle_value, cpu_use):
	pass


if __name__ == '__main__':
	main()
