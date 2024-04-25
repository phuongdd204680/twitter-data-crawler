import time

from utils.logger_utils import get_logger
from utils.time_utils import round_timestamp

logger = get_logger('CLI Job')

SLEEP_DURATION = 3


class CLIJob:
    """
    Base for jobs that need to be run continually
    Deprecated. Use SchedulerJob instead.
    """

    def __init__(self, interval=None, period=None, limit=None, end_timestamp=None, retry=True):
        """
        Args:
            * interval: Specify the time interval between each job
            * end_timestamp: the timestamp that the job should stop. Left to 'None' if you don't want it to stop
            * retry=True: Determine whether the function should retry if there is an error
        """
        self.interval = interval
        self.period = period
        self.limit = limit
        self.end_timestamp = end_timestamp

        self.retry = retry

    def run(self, *args, **kwargs):
        self._pre_start()
        while True:
            try:
                self._start()
                self._execute(*args, **kwargs)
            except Exception as ex:
                logger.exception(ex)
                logger.warning('Something went wrong!!!')
                if self.retry:
                    self._retry()
                    continue

            self._end()

            # Check if not repeat
            if not self.interval:
                break

            # Check if finish
            next_synced_timestamp = self._get_next_synced_timestamp()
            if self._check_finish(next_synced_timestamp):
                break

            # Sleep to next synced time
            time_sleep = next_synced_timestamp - time.time()
            if time_sleep > 0:
                logger.info(f'Sleep {round(time_sleep, 3)} seconds')
                time.sleep(time_sleep)

        self._follow_end()

    def _get_next_synced_timestamp(self):
        # Get the next execute timestamp
        return round_timestamp(int(time.time()), round_time=self.interval) + self.interval

    def _pre_start(self):
        # Declare object variables and prepare data
        pass

    def _start(self):
        # Before execute
        pass

    def _end(self):
        # After execute
        pass

    def _follow_end(self):
        # End job, export results or close connections
        pass

    def _check_finish(self, next_synced_timestamp):
        # Check if not repeat
        if self.interval is None:
            return True

        # Check if over end timestamp
        if (self.end_timestamp is not None) and (next_synced_timestamp > self.end_timestamp):
            return True

        return False

    def _execute(self, *args, **kwargs):
        # Main execute handler
        pass

    def _retry(self):
        # Do before retry
        logger.warning(f'Try again after {SLEEP_DURATION} seconds ...')
        time.sleep(SLEEP_DURATION)
