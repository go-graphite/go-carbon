import whisper
import os
import time

def set_up_create():
	path = "/tmp/whisper-testing.wsp"
	try:
		os.remove(path)
	except:
		pass
	archive_list = [[1,300], [60,30], [300,12]]
	def tear_down():
		os.remove(path)
	
	return path, archive_list, tear_down

def benchmark_create_update_fetch():
	path, archive_list, tear_down = set_up_create()
	# start timer
	start_time = time.clock()
	for i in range(100):
		whisper.create(path, archive_list)

		seconds_ago = 3500
		current_value = 0.5
		increment = 0.2
		now = time.time()
		# file_update closes the file so we have to reopen every time
		for i in range(seconds_ago):
			whisper.update(path, current_value, now - seconds_ago + i)
			current_value += increment

		from_time = now - seconds_ago
		until_time = from_time + 1000

		whisper.fetch(path, from_time, until_time)
		tear_down()

	# end timer
	end_time = time.clock()
	elapsed_time = end_time - start_time

	print "Executed 100 iterations in %ss (%i ns/op)" % (elapsed_time, (elapsed_time * 1000 * 1000 * 1000) / 100)

if __name__ == "__main__":
	benchmark_create_update_fetch()
