# -*- coding: utf8 -*-
import logging
import signal
from Queue import Queue, Empty
from threading import Thread, current_thread, Lock
from pprint import pprint
from cStringIO import StringIO

log = logging.getLogger(__name__)


class MultiWorker(object):
    def __init__(self, workers):
        self.workers_total = workers
        self._worker_threads = None
        self._total_processed_lock = Lock()

    def _setup(self):
        raise NotImplemented

    def _cleanup(self):
        raise NotImplemented

    def _process_item(self, item):
        raise NotImplemented

    def _item_generator(self):
        raise NotImplemented

    def _job_size(self):
        raise NotImplemented

    def _report_completed(self, item, total_processed):
        pass

    def _increment_total_processed(self):
        with self._total_processed_lock:
            self._total_processed += 1
            return self._total_processed

    def _worker(self):
        my_worker_number = self._worker_threads.index(current_thread())

        while True:
            item = self._data_queue.get()
            log.debug('Worker %s: got item: %s.' % (my_worker_number, item))

            if item == 'QUEUE_DONE':
                log.debug('Worker %s done, exiting.' % my_worker_number)
                self._data_queue.task_done()
                break

            if self._need_to_stop():
                log.debug('Worker %s: need to stop, ignoring item.' % my_worker_number)
                self._data_queue.task_done()
                continue

            try:
                self._process_item(item)
                log.info('Worker %s: finished %s' % (my_worker_number, item))
            except Exception:
                self._had_errors = True
                itempp = StringIO()
                itempp.write('Worker %s: exception on item:\n' % my_worker_number)
                pprint(item, stream=itempp)
                log.exception(itempp.getvalue())

            log.debug('Worker %s: setting task done.' % my_worker_number)
            processed = self._increment_total_processed()
            self._data_queue.task_done()
            log.debug('incremented total processed, total is %s' % processed)
            self._report_completed(item, processed)

    def _setup_workers(self):
        if self._worker_threads is not None:
            return

        self._data_queue = Queue(maxsize=self.workers_total * 10)
        self._worker_threads = []

        log.debug('Setting up %s sync_data workers.' % self.workers_total)
        for i in xrange(self.workers_total):
            thread = Thread(target=self._worker)
            self._worker_threads.append(thread)
            thread.start()

    def _finish_workers(self, empty=False):
        if empty:
            log.debug('emptying queue.')
            while True:
                try:
                    item = self._data_queue.get(False)
                    log.debug('Removed item from queue without processing: %s' % item)
                    self._data_queue.task_done()
                except Empty:
                    break

        for i in xrange(len(self._worker_threads)):
            log.debug('sending queue done signal to worker %s' % i)
            self._data_queue.put('QUEUE_DONE')

        log.debug('blocking until pending finished')
        self._data_queue.join()
        log.debug('finished')
        self._worker_threads = None

    def _need_to_stop(self):
        return self._stop_all or (self._had_errors and self._stop_on_errors)

    def execute(self, stop_on_errors=True):
        self._setup_workers()

        if current_thread().getName() == 'MainThread':
            def rs(*args):
                log.critical('received interrupt, finalizing...')
                self._stop_all = True
            signal.signal(signal.SIGINT, rs)

        self._stop_all = False
        self._had_errors = False
        self._stop_on_errors = stop_on_errors
        self._total_processed = 0
        
        try:
            self._setup()

            for item in self._item_generator():
                if self._need_to_stop():
                    log.debug('stopped processing orders')
                    break

                self._data_queue.put(item)
                log.debug('added item %s to the queue' % item)

            if not self._need_to_stop():
                self._data_queue.join()

                if self._total_processed != self._job_size():
                    raise Exception('total processed(%s) != expected job size(%s)' %
                        (self._total_processed, self._job_size()))

            self._finish_workers(empty=self._need_to_stop())
        except:
            self._finish_workers(empty=True)
            self._had_errors = True
            log.exception('Error')
            raise
        finally:
            self._cleanup()

            if self._had_errors:
                log.critical('Execution finished with errors.')

            if not self._need_to_stop():
                log.info('Execution finished successfully.')

        return not self._had_errors and not self._stop_all
