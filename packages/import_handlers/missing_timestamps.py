import packages.redshift.all_timestamps as all_timestamps
import packages.mysql.imported_timestamps as imported_timestamps

import logging

class MissingTimestamps:
    def __init__(self):
        """ Grab all timestamps in raw redshift table
            and all in imported_timestamps from mysql """

        # Get all timestamps from redshift
        self.at = all_timestamps.AllTimestamps()
        self.all_timestamps = self.at.get_all_timestamps()

        # Get all previously imported timestamps
        self.it = imported_timestamps.ImportedTimestamps()
        self.imported_timestamps = self.it.get_imported_timestamps()

        # Get list of missing timestamps
        self.missing_timestamps = list(set(self.all_timestamps)
                                    - set(self.imported_timestamps))

        self.missing_timestamps.sort()
        logging.info('all missing timestamps')
        logging.info(self.missing_timestamps)


    def __call__(self):
        for i in range(len(self.missing_timestamps)):
            yield self.missing_timestamps[i]
