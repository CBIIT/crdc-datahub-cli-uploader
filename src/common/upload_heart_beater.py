#!/usr/bin/env python
import threading
import time

"""
class: UploadHeartBeater to send a heart beat to backend during uploading files.
"""
class UploadHeartBeater:
    def __init__(self, batch_id, graphql_client):
        self._graphql_client = graphql_client
        self._batch_id = batch_id
        self._beat_thread = None
        self._stop_event = threading.Event()

    """
    private function: _beat to call updateBatch API in backend to set validating flag to true per 5 min.
    """
    def _beat(self):
    
        while not self._stop_event.is_set():
            try:
                self._graphql_client.update_batch(self._batch_id, None, True)
                time.sleep(5*60)
            except Exception as e:
                print(f"Failed to update batch: {e}")
                time.sleep(5*60)  
            finally:  
                continue
    """
    public function: start to start the heart beater thread
    """
    def start(self):
        self._beat_thread = threading.Thread(target=self._beat)
        self._beat_thread.start()
    """
    public function: stop to stop the heart beater thread
    """
    def stop(self):
        self._stop_event.set()
        self._beat_thread.join()
        self._beat_thread = None
