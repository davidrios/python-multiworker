from time import sleep
from random import randint
from datetime import datetime
from math import ceil

from multiworker import MultiWorker

class ExampleProcessor(MultiWorker):
    def __init__(self, workers, items_to_process, sleep_time, error_rate):
        super(ExampleProcessor, self).__init__(workers)
        self._items_to_process = items_to_process
        self._error_rate = error_rate
        self._sleep_time = sleep_time

    def _setup(self):
        self._start_time = datetime.now()
        self._last_report = None
        print 'started things up.'

    def _cleanup(self):
        print 'did some house cleaning.'

    def _process_item(self, item):
        if self._error_rate > 0:
            if randint(0, ceil(1 / self._error_rate)) == 1:
                raise Exception('OMG!')

        sleep(self._sleep_time)

    def _item_generator(self):
        return xrange(self._items_to_process)

    def _job_size(self):
        return self._items_to_process

    def _report_completed(self, item, total_processed):
        # report estimated time every 3 seconds
        time_elapsed = datetime.now() - self._start_time
        if self._last_report is None:
            self._last_report = time_elapsed.seconds
            return

        if time_elapsed.seconds % 3 == 0 and time_elapsed.seconds != self._last_report:
            self._last_report = time_elapsed.seconds
            estimated = time_elapsed / total_processed * (self._job_size() - total_processed)
            print 'Estimated time remaining to complete: %s' % estimated


if __name__ == '__main__':
    import logging
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--verbose', '-v', action='count')
    parser.add_argument('--workers', type=int, default=4)
    parser.add_argument('--items', type=int, default=500)
    parser.add_argument('--sleep-time', type=float, default=0.2)
    parser.add_argument('--error-rate', type=float, default=0.001)
    parser.add_argument('--continue-on-errors', action='store_true')

    args = parser.parse_args()

    level = {
        None: logging.WARNING,
        1: logging.INFO,
        2: logging.DEBUG,
    }.get(args.verbose, logging.DEBUG)

    format = '%(asctime)s:%(levelname)s:%(name)s:%(message)s'
    logging.basicConfig(level=level, format=format)

    try:
        processor = ExampleProcessor(args.workers,
                                     args.items,
                                     args.sleep_time,
                                     args.error_rate)
        processor.execute(stop_on_errors=not args.continue_on_errors)
    finally:
        logging.shutdown()
