#I have imported everything that I believe we will need for the project
import psutil #I have never used this library before, make sure you pip install it (i have already done this for our regular vm, i do not know the password for the -s vm)
import threading #make sure to set all spawned threads to daemon threads, otherwise everything locks up on you
import socket #easymode socket library
import argparse #this library just grabs command line arguments, its extremely powerful but has confusing documentation
import pickle #use this library for transfering jobs -- sock.send(pickle.dumps(data)), pickle.loads(sock.recv(num_bytes))
import Queue #if you dequeue an empty queue this will block unless you set the parameter right, be careful
from messages import MessageManager
import sys
import time

# http://stackoverflow.com/questions/5998245/get-current-time-in-milliseconds-in-python
current_milli_time = lambda: int(round(time.time() * 1000))

#global throttle variable to be accessed by any thread
throttle = 1.0
job_queue = Queue.Queue()
done_jobs = Queue.Queue()

got_user_input = 0
# vars that are (probably) not thread safe
message_manager = None
stopping = False

class Job:
	def __init__(self, job_id, data_slice):
		self.job_id = job_id
		self.data = data_slice

	def compute(self):
		for j in xrange(1000):
			for i, el in enumerate(self.data):
				self.data[i] = el + 1.111111

#make sure to install psutil before running
def main():
	global message_manager, stopping, throttle

	parser = argparse.ArgumentParser()
	parser.add_argument("host")
	parser.add_argument("port")
	parser.add_argument('--node', choices=['remote', 'local'], nargs=1, type=str, required=True)

	#node now contains the string value 'remote' or 'local'
	args = parser.parse_args()
	node = args.node[0]
	host = args.host
	port = int(args.port)

	if node == 'remote':
		# throttle = 0.5
		message_manager = MessageManager(host, port, slave=True)

	else:
		message_manager = MessageManager(host, port)
		bootstrap_phase()

	processing_phase()

	if node == 'remote':
		message_manager.shutdown()

# assumes only called from the local node
def bootstrap_phase():
	jobs = []

	# total_size = 1024*1024*32
	# num_jobs = 512
	total_size = 1024*32
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
	message_manager.write_array_of_jobs(other_half)

def processing_phase():
	# launch worker thread
	worker = threading.Thread(target=worker_thread)
	worker.daemon = True

	worker.start()

	# launch load balancer
	print "Waiting for message"
	message = message_manager.read_message()

	while True:
		if message['type'] == 'job':
			job_queue.put(message['payload'])

		if message['type'] == 'alert':
			other_queue = message['payload']
			print other_queue
			if other_queue == 0:
				message_manager.write_done()
				break

		if message['type'] == 'done':
			print 'DONE'
			break

		message = message_manager.read_message()

	print "out of the loop"
	stopping = True
	worker.join()

def aggregation_phase():
	#transfer all results from remote to local node

	pass

# @293
# @329
# assuming every job takes about the same amount of time to process, if we sleep
# for x % of the last jobs processing time we will get roughly the throttling we
# have asked for
def worker_thread():
	global stopping
	jobs_seen = 0

	job = job_queue.get()

	while not stopping or not job_queue.empty():
		if job != None:
			jobs_seen += 1

			before = current_milli_time()

			job.compute()
			done_jobs.put(job)

			after = current_milli_time()

			elapsed = after - before
			sleep_amount = elapsed * (1.0 - throttle)

			time.sleep(sleep_amount / 1000.0)

			# print ('\rprocessing job: %d' % job.job_id),
			# print ("sleeping for %f" % sleep_amount),
			sys.stdout.flush()

			message_manager.write_alert(job_queue.qsize())

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

def listen_for_user():
	while not stopping:
		u_input = raw_input('Enter throttle value: ')
		got_user_input = 1
		throttle = got_user_input

def hardware_monitor():
	user_thread = threading.Thread(target=listen_for_user)
	
	usage = psutil.cpu_percent(interval=1)
	


if __name__ == '__main__':
	main()
