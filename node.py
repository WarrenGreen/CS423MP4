#I have imported everything that I believe we will need for the project
import psutil
import threading
import socket
import argparse

#make sure to install psutil before running
def main():
	parser = argparse.ArgumentParser()
	parser.add_argument('--throttle', nargs=1, type=float)

	#throttle now contains the float value supplied by the user
	throttle = parser.parse_args().throttle[0]

	#initialize vector A
	A = []
	for i in range(1024*1024*32):
		A.append(1.111111)


#write all helper functions here
#this is a high level overview that might help, feel free to change
def transfer_manager():
	pass
def worker_thread():
	pass
def state_manager():
	pass
def adaptor():
	pass
def hardware_monitor(throttle_value, cpu_use):
	pass


if __name__ == '__main__':
	main()