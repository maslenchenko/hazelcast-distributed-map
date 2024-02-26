import hazelcast
import threading
import time

def create_client():
    client = hazelcast.HazelcastClient()
    return client

def writer(queue, write_complete_event):
    for i in range(1, 101):
        put_success = queue.offer(i)
        current_size = queue.size()
        if put_success:
            print(f'Writer: Added {i} to the queue; Current size: {current_size}')
        else:
            print(f'Writer: Queue is full. Could not add {i}')
        time.sleep(0.5) 
    write_complete_event.set()

def reader(queue, reader_id, write_complete_event):
    while not write_complete_event.is_set() or not queue.is_empty():
        try:
            item = queue.poll(1)  
            if item is None:
                if write_complete_event.is_set():
                    break
                else:
                    continue 
            print(f'Reader {reader_id}: Took {item} from the queue')
        except hazelcast.exception.HazelcastInstanceNotActiveError:
            break
        time.sleep(1.5)  

if __name__ == '__main__':
    client = create_client()
    queue = client.get_queue('my-bounded-queue').blocking()
    queue.clear() 
    
    write_complete_event = threading.Event()

    writer_thread = threading.Thread(target=writer, args=(queue, write_complete_event))
    writer_thread.start()

    reader_threads = []
    for reader_id in range(1, 3):
        thread = threading.Thread(target=reader, args=(queue, reader_id, write_complete_event))
        thread.start()
        reader_threads.append(thread)

    writer_thread.join()

    for thread in reader_threads:
        thread.join()

    print('All threads have finished executing.')

    queue.clear()

    writer_thread = threading.Thread(target=writer, args=(queue, write_complete_event))
    writer_thread.start()

    writer_thread.join()

    print('Writer has finished. No readers were started, so the queue should be full.')
