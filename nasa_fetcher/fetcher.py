import os
import logging
import subprocess
from datetime import date, timedelta

import numpy as np

LOGGER = logging.getLogger("NASA-Fetcher")
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


def fetch_modis_LST(dst_dir, cookies_dir, date_start, date_end, satellite="aqua"):
    """
    Fetch MODIS LST dataset
    :param dst_dir:
    :param cookies_dir:
    :param date_start:
    :param date_end:
    :return:
    """
    assert os.path.isdir(dst_dir), "The destination path is not a valid directory"
    assert os.path.isfile(cookies_dir)
    assert isinstance(date_start, date)
    assert isinstance(date_end, date)
    assert date_start <= date_end
    assert satellite in ["aqua", "terra"]

    if satellite == "aqua":
        sat_dir = "MOLA"
        product_prefix = "MYD"
    else:
        sat_dir = "MOLT"
        product_prefix = "MOD"
    lst_prod_day = product_prefix + "21A1D.006"
    lst_prod_night = product_prefix + "21A1N.006"

    files = ["h08v04", "h08v05"]

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
        for i, lst_prod in enumerate([lst_prod_day, lst_prod_night]):
            url = "https://e4ftl01.cr.usgs.gov/{0}/{1}/{2}/".format(sat_dir,
                                                                    lst_prod,
                                                                    temp_date.strftime("%Y.%m.%d"))
            for f in files:
                ls_args = ['wget', '-q', '-nH', '-nd', '-O', '-', url, '|', 'grep', '--line-buffered', f, '|',
                           'cut', '-f4', '-d\\']
                output = os.popen(" ".join(ls_args))
                for line in output.readlines():
                    fn = line.split("</a>")[0].split(">")[-1]
                    if fn[-3:] == "hdf" or fn[-3:] == "xml":
                        d_url = url + fn
                        d_fn = temp_date.strftime("%Y%m%d") + "_" + lst_prod + "_" + f + "." + fn[-3:]
                        download_args = ['wget', '--load-cookies', cookies_dir, '--save-cookies', cookies_dir,
                                         '--keep-session-cookies', '-O',
                                         os.path.join(dst_dir_year_day, d_fn), d_url]
                        cmd = " ".join(download_args)
                        output = os.popen(cmd)
        temp_date += timedelta(days=1)
    return


def fetch_modis_FSC(dst_dir, cookies_dir, date_start, date_end, satellite="terra"):
    """
    Fetch MODIS LST dataset
    :param dst_dir:
    :param cookies_dir:
    :param date_start:
    :param date_end:
    :return:
    """
    assert os.path.isdir(dst_dir), "The destination path is not a valid directory"
    assert os.path.isfile(cookies_dir)
    assert isinstance(date_start, date)
    assert isinstance(date_end, date)
    assert date_start <= date_end
    assert satellite in ["aqua", "terra"]

    if satellite == "aqua":
        sat_dir = "MOSA"
        product_prefix = "MYD"
    else:
        sat_dir = "MOST"
        product_prefix = "MOD"
    fsc_prod = product_prefix + "10A1.006"

    files = ["h08v04", "h08v05"]

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
        url = "https://n5eil01u.ecs.nsidc.org/{0}/{1}/{2}/".format(sat_dir, fsc_prod, temp_date.strftime("%Y.%m.%d"))
        print url
        for f in files:
            fn_pattern = "'*{0}*'".format(f)
            download_args = ['wget', '--load-cookies', cookies_dir, '--save-cookies', cookies_dir,
                             '--keep-session-cookies', '--no-check-certificate', '--auth-no-challenge=on',
                             '-r', '-np', '-nH', '--cut-dirs=3', '-e', 'robots=off', '-A', fn_pattern, '-R jpg', '-P',
                             dst_dir_year_day, url]
            cmd = " ".join(download_args)
            output = os.popen(cmd)
        temp_date += timedelta(days=1)
    return


def fetch_czo_lidar(dst_dir, area, snowon=True):
    if snowon:
        url = "https://cloud.sdsc.edu/v1/AUTH_opentopography/PC_Bulk/SSCZO_Mar10/"
        ls_args = ['wget', '-O', '-', url, '|', "grep", "--line-buffered", "ot_SS1_A{0}".format(area)]
    else:
        url = "https://cloud.sdsc.edu/v1/AUTH_opentopography/PC_Bulk/SSCZO_Aug10/"
        ls_args = ['wget', '-O', '-', url, '|', "grep", "--line-buffered", "ot_SS2_A{0}".format(area)]
    ls_cmd = " ".join(ls_args)
    output = os.popen(ls_cmd)
    lines = output.readlines()
    for line in lines:
        fn = line.split("</a>")[0].split(">")[-1]
        download_args = ['wget', url + fn, "-P", dst_dir]
        download_cmd = " ".join(download_args)
        output = os.popen(download_cmd)
    return 0


def fetch_opentopo_lidar(dst_dir, fn_pattern, src_dir, easting_range, northing_range):
    if not os.path.isdir(dst_dir):
        os.mkdir(dst_dir)
    url = "https://cloud.sdsc.edu/v1/AUTH_opentopography/PC_Bulk/{0}/".format(src_dir)
    fns = []
    for easting in np.arange(easting_range[0], easting_range[1]+1000, 500):
        for northing in np.arange(northing_range[0], northing_range[1]+1000, 1000):
            fn = fn_pattern.format(int(easting), int(northing))
            fns.append(fn)
    for fn in fns:
        download_args = ['wget', url + fn, "-P", dst_dir]
        download_cmd = " ".join(download_args)
        try:
            output = os.popen(download_cmd)
        except:
            pass
    return 0


# fetch_nldas('/home/zeshi/dev/NASA_DATA/data', '/home/zeshi/.urs_cookies', date(2016, 1, 1), date(2017, 9, 1))
# fetch_czo_lidar("/home/zeshi/dev/NASA_DATA/data/czo_lidar/", 3)

# fetch_modis_LST("/home/zeshi/dev/NASA_DATA/data/LST",
#                 "/home/zeshi/.urs_cookies",
#                 date(2013, 9, 30),
#                 date(2013, 9, 30),
#                 satellite="terra")

# fetch_czo_lidar("/media/raid0/zeshi/LAStools/data/a1_lidar_snowoff", 1, snowon=False)
# # fetch_czo_lidar("/media/raid0/zeshi/LAStools/data/a2_lidar_snowon", 2)
# fetch_czo_lidar("/media/raid0/zeshi/LAStools/data/a2_lidar_snowoff", 2, snowon=False)
# # fetch_czo_lidar("/media/raid0/zeshi/LAStools/data/a6_lidar_snowon", 6)
# fetch_czo_lidar("/media/raid0/zeshi/LAStools/data/a6_lidar_snowoff", 6, snowon=False)

# fetch_opentopo_lidar("/media/raid0/zeshi/LAStools/data/boulder_snowon", "ot_{0}_{1}.laz", "BCZO_SnowOn05",
#                      [450000, 455000], [4427000, 4430000])
fetch_opentopo_lidar("/media/raid0/zeshi/LAStools/data/boulder_snowoff", "ot_{0}_{1}.laz", "BcCZO_10_Aug",
                     [450000, 455000], [4427000, 4430000])
# fetch_opentopo_lidar("/media/raid0/zeshi/LAStools/data/jemez_snowon", "ot_{0}_{1}.laz", "JRB_10_Mar",
#                      [355000, 361000], [3968000, 3972000])
# fetch_opentopo_lidar("/media/raid0/zeshi/LAStools/data/jemez_snowoff", "ot_{0}_{1}_1.laz", "JRB_10_Jul",
#                      [355000, 361000], [3968000, 3972000])

# fetch_modis_FSC("/home/zeshi/dev/NASA_DATA/data/FSC", "/home/zeshi/.urs_cookies", date(2000, 2, 24),
#                 date(2017, 11, 19), satellite="terra")
