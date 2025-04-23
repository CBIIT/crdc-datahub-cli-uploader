#!/usr/bin/env python
import threading

"""
class: UploadHeartBeater to send heartbeat to backend during uploading files.
"""
class UploadHeartBeater:
    def __init__(self, batch_id, graphql_client, heartbeat_interval=300):
        self.graphql_client = graphql_client
        self.batch_id = batch_id
        self.beat_thread = None
        self.stop_event = threading.Event()
        self.heartbeat_interval = heartbeat_interval

    """
    private function: beat 
    call updateBatch API in backend by set uploading to true per 5 min.
    """
    def __beat(self):
    
        while not self.stop_event.is_set():
            try:
                self.graphql_client.update_batch(self.batch_id, None, "true")
            except Exception as e:
                print(f"Failed to update batch: {e}")
            finally:
                 # make the thread sleep for 5 min
                self.stop_event.wait(self.heartbeat_interval)
    """
    public function: start the heartbeat thread
    """
    def start(self):
        self.beat_thread = threading.Thread(target=self.__beat)
        self.beat_thread.start()
    """
    public function: stop the heartbeat thread
    """
    def stop(self):
        self.stop_event.set()
        self.beat_thread.join()
