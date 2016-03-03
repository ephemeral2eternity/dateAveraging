from datetime import datetime, timedelta
from csv2json import *

def search_twitter_records(twitter_list, searchWin, searchDate, searchTicker):
    searched_items = []
    for twitter_item in twitter_list:
        cur_date_str = twitter_item["asof_date"]
        cur_date = datetime.strptime(cur_date_str, "%m/%d/%Y")
        date_delta = searchDate - cur_date

        cur_ticker = twitter_item["symbol"]

        if (date_delta.days >= 0) and (date_delta < timedelta(days=searchWin)) and (cur_ticker == searchTicker):
            searched_items.append(twitter_item)

    return searched_items


recom_file = "/home/chenw/data/recom.csv"
twitter_file = "/home/chenw/data/twitter_noretweets.csv"
# twitter_file = "/home/chenw/data/twitter_retweets.csv"
outfile = "/home/chenw/data/recom_noretweets_merged.csv"
# outfile = "/home/chenw/data/merged.csv"

recom_list = loadCSV2List(recom_file)
twitter_list = loadCSV2List(twitter_file)

searchWin = 1

output_recom_fields = ["CUSIP", "OFTIC", "ANNTIMS", "ANNDATS", "CNAME", "ACTDATS", "ACTTIMS", "REVDATS", "REVTIMS",
                 "EMASKCD", "ESTIMID", "AMASKCD", "ANALYST", "IRECCD"]

output_twitter_fields = ["bullish_intensity", "bearish_intensity", "bull_minus_bear", "bull_scored_messages", "bear_scored_messages",
                         "bull_bear_msg_ratio", "total_scanned_messages"]

new_fields = ["NEWDATE", "OFFDAY", "RECOMSCALE", "CONSISTENCY"]
output_fields = output_recom_fields + output_twitter_fields + new_fields

## Create csvfile
with open(outfile, 'wb') as out_csv_file:
    out_csv_writer = csv.DictWriter(out_csv_file, fieldnames=output_fields)
    out_csv_writer.writeheader()

    ## Process recom_list items one by one
    # new_list = []

    for recom_item in recom_list:
        new_item = {}
        ## Define all new fields in the output csv
        newdateField = "NEWDATE"
        offdayField = "OFFDAY"
        recomscaleField = "RECOMSCALE"
        consistencyField = "CONSISTENCY"

        ## Get New Date variable
        curDateStr = recom_item["ANNDATS"]
        cur_date = datetime.strptime(curDateStr, "%Y%m%d")

        curTimeStr = recom_item["ANNTIMS"]
        cur_time = datetime.strptime(curTimeStr, "%H:%M:%S")

        if cur_time.hour > 16:
            new_date = cur_date + timedelta(days=1)
        else:
            new_date = cur_date

        new_date_string = new_date.strftime("%Y%m%d")
        new_item[newdateField] = new_date_string

        ## Get Offday variable
        if cur_time.hour > 16:
            offday = 1
        else:
            offday = 0
        new_item[offdayField] = offday

        ## Get recom_scale variable
        ireccd = recom_item["IRECCD"]
        if ireccd > 3:
            recom_scale = 1
        elif ireccd == 3:
            recom_scale = 0
        else:
            recom_scale = -1
        new_item[recomscaleField] = recom_scale

        ## Copy recom fields
        for recom_field in output_recom_fields:
            new_item[recom_field] = recom_item[recom_field]

        ## Get company ticker
        official_ticker = recom_item["OFTIC"]
        records_in_window = search_twitter_records(twitter_list, searchWin, new_date, official_ticker)

        ## Averaging the fields in searched twitter items
        if not records_in_window:
            for twitter_field in output_twitter_fields:
                new_item[twitter_field] = "NaN"
        else:
            for twitter_field in output_twitter_fields:
                cur_field_list = []
                for searched_item in records_in_window:
                    cur_field_list.append(float(searched_item[twitter_field]))

                cur_field_val = sum(cur_field_list) / float(len(cur_field_list))
                new_item[twitter_field] = cur_field_val

        ## Compute consistency variable
        bull_minus_bear_ave = new_item["bull_minus_bear"]
        if bull_minus_bear_ave*recom_scale > 0:
            consistency = 1
        elif (bull_minus_bear_ave == 0) and (recom_scale == 0):
            consistency = 1
        else:
            consistency = 0
        new_item[consistencyField] = consistency

        print "Row to write: ", new_item
        out_csv_writer.writerow(new_item)
