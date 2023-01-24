# This file contains functions to download and save the database of the traffic commissioner to view registered bus services. Currently England only
# LLD please include standard header with version, update date, author etc. Brief overview of order of execution and explanation of
# functions would also be useful.
# please ensure the functions are written in a logical order, e.g. main function that calls others, then the other functions either
# in the order they're called or most functional down to least functional ('helper' functions). To me it seems this
# would best be refactored as 1) get_odc_db(save=True) ((see explanation below)),
# then a much simpler save() function which combines the today/download folder/save code.


import pandas as pd
from datetime import date
import requests
import io
import os
from pathlib import Path
from sys import platform

# LLD will these ever change? for readability please save a base URL (up to the word export) and then base url +
# the csv part for each area

west_england = 'https://content.mgmt.dvsacloud.uk/olcs.prod.dvsa.aws/data-gov-uk-export/Bus_RegisteredOnly_H.csv'
west_midlands = 'https://content.mgmt.dvsacloud.uk/olcs.prod.dvsa.aws/data-gov-uk-export/Bus_RegisteredOnly_D.csv'
london_south_east = 'https://content.mgmt.dvsacloud.uk/olcs.prod.dvsa.aws/data-gov-uk-export/Bus_RegisteredOnly_K.csv'
north_west_england = 'https://content.mgmt.dvsacloud.uk/olcs.prod.dvsa.aws/data-gov-uk-export/Bus_RegisteredOnly_C.csv'
north_east_england = 'https://content.mgmt.dvsacloud.uk/olcs.prod.dvsa.aws/data-gov-uk-export/Bus_RegisteredOnly_B.csv'
east_england = 'https://content.mgmt.dvsacloud.uk/olcs.prod.dvsa.aws/data-gov-uk-export/Bus_RegisteredOnly_F.csv'

otc_db_files = [west_england, west_midlands, london_south_east,
                north_west_england, north_east_england, east_england]

today = str(date.today())

# LLD consider allowing user to choose preferred folder for download, or downloading to user's current directory


def get_user_downloads_folder():
    if platform == "win32":
        downloads_folder = str(Path.home() / "Downloads")

    elif platform == "darwin" or "linux":
        # for macOS or linux
        downloads_folder = str(os.path.join(Path.home(), "Downloads"))

    else:
        print("Unrecognised OS, cannot locate downloads folder")
        downloads_folder = ""

    return downloads_folder

# LLD - doing extra logic. make the check for exists first, then if not create the folder.
# use standard methods for checking if exists, see below link


def create_today_folder():
    '''
    Create a folder, named with the days data, so that timetables can be saved locally
    '''

    today = str(date.today())

    downloads_folder = get_user_downloads_folder()

# LLD this will result in an uncaught exception if downloads doesn't exist - you have provided a print message at Line 33 which
# goes some way to helping the user, however it won't appear in the StackTrace.
# Better approach: - get rid of the else block from line 32, - enclose the below in a try/catch (catch the
# specific exception), - first, allow user to choose a download folder; - if still an exception, set the exception
# error message to your message at Line 33

# LLD - please replace the below with one of the following code options for best practice - user doesn't need a feedback message
# https://www.tutorialspoint.com/How-can-I-create-a-directory-if-it-does-not-exist-using-Python. Combine steps for neater
# code where possible
    # list out the file names in the downloads folder - LLD this isn't needed - you would use path.exists(path) instead
    files = os.listdir(downloads_folder)

    # create the path for today folder in downloads
    today_folder_path = downloads_folder + '/' + today

    # if timetable output folder is not in downloads, create
    if today not in files:
        os.mkdir(today_folder_path)

    else:
        print('file with todays date already exists')

    return today_folder_path

# LLD code blocks should never be duplicated. This function is
# identical to fetch_otc_db() apart from the last few lines.
# There are two options:
# -call fetch_otc_db() within save_otc_db(), then save the result
# - have one method with a default/optional argument for save option,
#  e.g. get_otc_db(save=True)


def save_otc_db():
    # instantiate a list in which each regions dataframe will reside
    otc_regions = []

    # loop through regions as per OTC DB page
    for region in otc_db_files:
        # print(f'Downloading region: {region}...')
        # get the raw text from the link, should be a csv file
        text_out = requests.get(region).content

        # convert to a dataframe
        df = pd.read_csv(io.StringIO(text_out.decode('utf-8-sig')))

        # append current region df to regions list
        otc_regions.append(df)

    # combine to a single dataframe
    print('Merging files...')
    otc_db = pd.concat(otc_regions)
    # otc_db.drop(otc_db.columns[0], axis = 1, inplace = True)
    otc_db['service_code'] = otc_db['Reg_No'].str.replace('/', ':')

    # postgresql does not like uppercase or spaces - removing from column titles
    otc_db.columns = [c.lower() for c in otc_db.columns]
    otc_db.columns = [c.replace(" ", "_") for c in otc_db.columns]

    # remove duplicate rows
    otc_db = otc_db.drop_duplicates()

    create_today_folder()
    downloads_folder = get_user_downloads_folder()

    # LLD presumably if the user executes multiple calls to the API with the extractor object,
    # we don't need to run this every time - worth checking if this file path is already present (earlier on?)
    save_loc = downloads_folder + '/' + today + f'/otc_db_{today}.csv'

    otc_db.to_csv(save_loc, index=False)
    return otc_db


def fetch_otc_db():
    # instantiate a list in which each regions dataframe will reside
    otc_regions = []
    print(f'Downloading otc database...\n')
    # loop through regions as per OTC DB page
    for region in otc_db_files:
        # print(f'Downloading region: {region}...')
        # get the raw text from the link, should be a csv file
        text_out = requests.get(region).content

        # convert to a dataframe
        df = pd.read_csv(io.StringIO(text_out.decode('utf-8-sig')))

        # append current region df to regions list
        otc_regions.append(df)

    # combine to a single dataframe
    print('Merging files...')
    otc_db = pd.concat(otc_regions)
    # otc_db.drop(otc_db.columns[0], axis = 1, inplace = True)
    otc_db['service_code'] = otc_db['Reg_No'].str.replace('/', ':')

    # postgresql does not like uppercase or spaces - removing from column titles
    otc_db.columns = [c.lower() for c in otc_db.columns]
    otc_db.columns = [c.replace(" ", "_") for c in otc_db.columns]

    # remove duplicate rows
    otc_db = otc_db.drop_duplicates()

    return otc_db


if __name__ == "__main__":
    save_otc_db()