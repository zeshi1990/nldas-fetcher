import os
import logging
import subprocess
from datetime import date, timedelta

LOGGER = logging.getLogger("NldasFetcher")
LOGGER.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s')
ch.setFormatter(formatter)
LOGGER.addHandler(ch)


def fetch_nldas(dst_dir, cookies_dir, date_start, date_end):
    """
    Fetch NLDAS data day-by-day

    Parameters
    ----------
    dst_dir : str, destination path
    cookies_dir : str, cookies directory
    date_start : starting date, date instance
    date_end : ending date (inclusive), date instance
    """
    assert os.path.isdir(dst_dir), "The destination path is not a valid directory"
    assert os.path.isfile(cookies_dir)
    assert isinstance(date_start, date)
    assert isinstance(date_end, date)
    assert date_start <= date_end

    # Generate all days folder and url
    temp_date = date_start
    while temp_date <= date_end:
        year = temp_date.year
        doy = (temp_date - date(temp_date.year, 1, 1)).days + 1
        dst_dir_year = os.path.join(dst_dir, str(year))
        if not os.path.isdir(dst_dir_year):
            os.mkdir(dst_dir_year)
        dst_dir_year_day = os.path.join(dst_dir_year, str(doy).zfill(3))
        if not os.path.isdir(dst_dir_year_day):
            os.mkdir(dst_dir_year_day)
        url = "https://hydro1.gesdisc.eosdis.nasa.gov/data/NLDAS/" \
              "NLDAS_FORA0125_H.002/{0}/{1}/".format(year, str(doy).zfill(3))
        args = ['wget', '--load-cookies', cookies_dir, '--save-cookies', cookies_dir, '--keep-session-cookies', '-r',
                '-c', '-nH', '-nd', '-np', '-A', 'grb', '-P', dst_dir_year_day, '"{0}"'.format(url)]
        cmd_args = " ".join(args)
        LOGGER.info(cmd_args)
        output = os.popen(cmd_args)
        LOGGER.info(output)
        temp_date += timedelta(days=1)


fetch_nldas('/home/zeshi/dev/NLDAS/data', '/home/zeshi/.urs_cookies', date(2013, 1, 1), date(2014, 12, 31))
