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

tolerance = 0.00001
# vars that are (probably) not thread safe
message_manager = None
stopping = False
sent_done = False

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
		throttle = 0.1
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

	#total_size = 1024*1024*32
	#num_jobs = 512
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
	hardware_monitor()

def processing_phase():
	global stopping
	# launch worker thread
	worker = threading.Thread(target=worker_thread)
	worker.daemon = True

	worker.start()

	# launch load balancer
	print "Waiting for message"
	message = message_manager.read_message()

	while True:
		if message == None:
			continue

		if message['type'] == 'job':
			job_queue.put(message['payload'])
			sent_done = False

		if message['type'] == 'alert':
			other_queue = message['payload']
			qsize = job_queue.qsize()

			jobs_to_send = []

			if qsize > other_queue:
				diff = qsize - other_queue
				jobs_to_send = [job_queue.get() for i in xrange(diff/2)]

			if len(jobs_to_send) > 0:
				print "\nSending %d jobs to other machine" % len(jobs_to_send)
				message_manager.write_array_of_jobs(jobs_to_send)

		if message['type'] == 'done':
			print 'DONE'
			if(sent_done):
				stopping = True
				break
			elif(job_queue.qsize() > 0):
				message_manager.write_array_of_jobs(job_queue.qsize() /2)
			else:
				message_manager.write_done()
				stopping = True
				break

		message = message_manager.read_message()

	print "out of the loop"
	stopping = True
	worker.join()

	aggregation_phase()


def aggregation_phase():
	#transfer all results from remote to local node
	if message_manager.slave:
		jobs_to_send = [done_jobs.get() for i in xrange(0,done_jobs.qsize())]
		message_manager.write_array_of_jobs(jobs_to_send)
	else:
		message = message_manager.read_message()
		print message
		done_jobs.put(message['payload'])
		sums = []
		while not done_jobs.empty():
			curr_job = done_jobs.get()
			curr_sum = 0

			print curr_job
			for i, el in enumerate(curr_job.data):
				curr_sum += curr_job.data[i]

			sums.append(curr_sum)
		correct = 1

		for s in sums:
			for r in sums:
				if abs(s-r) > tolerance:
					correct = 0

		if correct:
			print "Aggregation was successful"
		else:
			print "Aggregation was not successful"

# @293
# @329
# assuming every job takes about the same amount of time to process, if we sleep
# for x % of the last jobs processing time we will get roughly the throttling we
# have asked for
def worker_thread():
	global stopping
	global sent_done
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

			print ('\rprocessing job: %d' % job.job_id),
			print ("sleeping for %f" % sleep_amount),
			print ("qsize %d" % job_queue.qsize()),
			sys.stdout.flush()

			message_manager.write_alert(job_queue.qsize())

		try:
			job = job_queue.get(timeout=5)
		except Queue.Empty:
			message_manager.write_done()
			sent_done = True
			job = None

	print
	print 'Saw %d jobs' % jobs_seen

def listen_for_user():
	while not stopping:
		u_input = raw_input('Enter throttle value: ')
		got_user_input = 1
		throttle = got_user_input

def hardware_monitor():
	user_thread = threading.Thread(target=listen_for_user)
	# while not stopping:
	# 	time.sleep(10)
	# 	usage = psutil.cpu_percent(interval=1)
	# 	print ('CPU Utilization: %d %' % usage)


if __name__ == '__main__':
	main()
