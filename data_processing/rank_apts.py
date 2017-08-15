#!/usr/bin/env python
from config import path_to_data, crawl_data_file, pre_processed_file, \
    crawl_command_dir, crawl_data_input_file
from datetime import datetime
from distanceCalculator import DistanceCalculator
import json
import os
from os.path import isfile, join
import pandas as pd
from pandas.io.json import json_normalize
from pprint import pprint
import sys

data_full_path = join(path_to_data, crawl_data_file)
pre_processed_full_path = join(path_to_data, pre_processed_file)
crawl_data_input = join(path_to_data, crawl_data_input_file)


def filter_bed_room_number(num_bedroom, data):
    return data[data['bedroom_num']>=num_bedroom]

def filter_bath_room_number(num_bathroom, data):
    return data[data['bathroom_num']>=num_bathroom]

def filter_walking_min_to_station(minute, data):
    print(data.columns)
    if not data.empty:
        return data[data['time_to_shuttle']<=minute]

def filter_apt_price(price, data):
    return data[data['min_rent']<=price]

def filter_distance_to_fb(distance, data):
    return data[data['distance_to_fb']<=distance]

def has_similar_feature(desired_feature, featureList):
    for feature in strList:
        if desired_feature in feature.lower():
            return True
    return False

def filter_no_washer_dryer(data):
    return data[data['feature_list'].apply(lambda x: has_similar_feature('washer', x))]

def filter_no_ac(data):
    return data[data['feature_list'].apply(lambda x: has_similar_feature('air c', x))]

def preprocess_data(data):
    pre_processed_file_exist = isfile(pre_processed_full_path)
    if pre_processed_file_exist:
        data = pd.read_json(pre_processed_full_path, orient='records')
    extra_columns = [
        'time_to_shuttle',
        'distance_to_fb',
        'time_to_fb_at_9',
        'time_from_fb_at_18'
    ]
    columns_exist = [
        (column in data.columns) for column in extra_columns
    ]

    need_pre_processing = not all(columns_exist)

    if need_pre_processing:
        distance_calc = DistanceCalculator()
        if isfile(pre_processed_full_path):
            os.remove(pre_processed_full_path)
        for index, exists in enumerate(columns_exist):
            if not exists:
                column = extra_columns[index]
                if column == 'time_to_shuttle':
                    try:
                        print('calculating time_to_shuttle...')
                        print(distance_calc.find_approx_station_with_shortest_time(data['address'][1]))
                        data = data.merge(data['address'].apply(\
                            lambda x: pd.Series(distance_calc.find_approx_station_with_shortest_time(x))),
                            left_index=True,
                            right_index=True)
                        print('Finished calculatng time_to_shuttle. Save cache...')
                        distance_calc.save_cache()
                    except:
                        print('Exception with time_to_shuttle calculation...')
                        raise
                elif column == 'distance_to_fb':
                    try:
                        print('calculating distance_to_fb...')
                        data['distance_to_fb'] = \
                            data['address'].apply(\
                                lambda x: distance_calc.calculate_distances_or_durations_to_dest(x))
                        print('Finished calculatng distance_to_fb. Save cache...')
                        distance_calc.save_cache()
                    except:
                        print('Exception with distance_to_fb calculation...')
                        raise
                elif column == 'time_to_fb_at_9':
                    try:
                        print('calculating time_to_fb_at_9...')
                        data['time_to_fb_at_9'] =\
                            data['address'].apply(\
                                lambda x: distance_calc.calculate_distances_or_durations_to_dest(\
                                    x, "driving", 9, False))
                        print('Finished calculatng time_to_fb_at_9. Save cache...')
                        distance_calc.save_cache()
                    except:
                        print('Exception with time_to_fb_at_9 calculation...')
                        raise
                elif column == 'time_from_fb_at_18':
                    try:
                        print('calculating time_from_fb_at_18')
                        data['time_from_fb_at_18'] =\
                            data['address'].apply(\
                                lambda x : distance_calc.calculate_distances_or_durations_from_dest(\
                                    x, "driving", 18, False))
                        print('Finished calculatng time_from_fb_at_18. Save cache...')
                        distance_calc.save_cache()
                    except:
                        print('Exception with time_from_fb_at_18 calcuation...')
                        raise
            data.to_json(path_or_buf=pre_processed_full_path, orient='records')
    return data

# main
def main(argv):
    import getopt
    def usage():
        print ('usage: %s [--bed=min_bedroom] [--bath=min_bathroom] [--walk=min_walking_time] [--avail_before=date] [--avail_after=date] [--price=max_price] [--dist=distance][--topk=k] [-w] [-a] [--clean]' % argv[0])
        return 100
    try:
        (opts, args) = getopt.getopt(argv[1:], 'aw', ['bed=', 'bath=', 'walk=', 'price=', 'dist=', 'avail_before=', 'avail_after=', 'clean'])
    except getopt.GetoptError:
        return usage()
    # need arg to output data
    if not args: return usage()

    optsKeys = [k for (k, v) in opts]

    if ('--clean' in optsKeys) and (isfile(data_full_path)):
        print("need to remove")
        os.remove(data_full_path)
        if isfile(pre_processed_full_path):
            os.remove(pre_processed_full_path)

    if ('--clean' in optsKeys) or (not isfile(data_full_path)):
        # recrawl data
        print("need to recrawl")
        cwd = os.getcwd()
        os.chdir(crawl_command_dir)
        os.system("scrapy crawl apartments -o ./findApartment/data/output.json -a input={}".format(crawl_data_input))
        os.chdir(cwd)

    with open(data_full_path) as apts_file:
       apts_info = json.load(apts_file)
    apts_data = json_normalize(apts_info)
    apts_data = preprocess_data(apts_data)

    print('finished pre processing data.')

    # sorting
    print('sorting data...')
    apts_data.sort_values(['min_rent'], inplace=True)

    topK = apts_data.shape[0]

    for (k, v) in opts:
        if k == '--bed':
            # filter out apts with < v bedrooms
            print('filter out apts with < {} bedrooms...'.format(v))
            apts_data = filter_bed_room_number(int(v), apts_data)
            print('Done!')
        elif k == '--bath':
            # filter out apts with < v bathrooms
            print('filter out apts with < {} bathrooms...'.format(v))
            apts_data = filter_bath_room_number(int(v), apts_data)
            print('Done!')
        elif k == '--walk':
            # filter out apts that are > v min to reach from any bus station
            print('filter out apts that are > {} min to reach from any shuttle station...'.format(v))
            apts_data = filter_walking_min_to_station(int(v), apts_data)
            print('Done!')
        elif k == '--price':
            # filter out apts with min_rent > v
            print('filter out apts with min_rent > ${}...'.format(v))
            apts_data = filter_apt_price(int(v), apts_data)
            print('Done!')
        elif k == '--dist':
            # filter out apts that are more than v miles from facebook
            print('filter out apts with distance more than {} miles from Facebook...'.format(v))
            apts_data = filter_distance_to_fb(int(v), apts_data)
            print('Done!')
        elif k == '--avail_before':
            print('save only apartments that are available before {}...'.format(v))
            apts_data = apts_data[apts_data['avail_date'] <= v]
            print('Done!')
        elif k == '--avail_after':
            print('save only apartments that are available after {}...'.format(v))
            apts_data = apts_data[apts_data['avail_date'] >= v]
        elif k == '--topk':
            # rank the apartments according to price and return the cheapest v of them
            topK = k
            # won't be appling cutoff here before all filtering are done
        elif k == '-w':
            # filter out apts with no washer/dryer
            print('filter out apts with no washer/dryer...')
            apts_data = filter_no_washer_dryer(apts_data)
            print('Done!')
        elif k == '-a':
            # filter out apts with no air conditioner
            print('filter out apts with no air conditioner...')
            apts_data = filter_no_ac(apts_data)
            print('Done!')

    apts_data = apts_data[:topK]

    fileName = join(path_to_data, args[0])
    if isfile(fileName):
        print('{} already exists, deleting it...'.format(fileName))
        os.remove(fileName)

    print('Saving result to {}...'.format(fileName))
    with open(fileName, 'w') as output_file:
        apts_data.to_csv(output_file)
    print('Done!')

if __name__ == '__main__': sys.exit(main(sys.argv))
