from config import google_api_key, loc, data_path, map_cache_data_path, \
    station_cache_data_path, geocode_cache_data_path
from datetime import timedelta, datetime
import googlemaps
import json
import math
import os
from os.path import isfile, join
import re

class DistanceCalculator:
    gmaps = googlemaps.Client(key=google_api_key)

    # make a cache to
    cache = {}
    cache_smallest_station = {}
    cache_geo_code = {}
    direction_api_calls = 0
    geocode_api_calls = 0
    def __init__(self):
        self.data_path = data_path
        with open(self.data_path) as dataFile:
            self.stations = json.load(dataFile)['locations']
        self.stations_geocode = [self.get_geocode_from_station(station) for station in self.stations]
        self.loc = loc
        self.map_cache_data_path = map_cache_data_path
        self.station_cache_data_path = station_cache_data_path
        self.geocode_cache_data_path = geocode_cache_data_path

        if isfile(self.map_cache_data_path):
            with open(self.map_cache_data_path) as cacheFile:
                self.cache = json.load(cacheFile)
        if isfile(self.map_cache_data_path):
            with open(self.station_cache_data_path) as cacheFile:
                self.cache_smallest_station = json.load(cacheFile)
        if isfile(self.geocode_cache_data_path):
            with open(self.geocode_cache_data_path) as cacheFile:
                self.cache_geo_code = json.load(cacheFile)
        now = datetime.now()
        self.departure_time = timedelta(days=(7-now.weekday())) + now

    def get_geocode_from_station(self, station_string):
        m = re.search('([\d.\-]+), ([\d.\-]+)', station_string)
        if m:
            return (float(m.group(1)), float(m.group(2)))

    def save_cache(self):
        if isfile(self.map_cache_data_path):
            print('deleting old map cache file...')
            os.remove(self.map_cache_data_path)
        if isfile(self.station_cache_data_path):
            print('deleting old station cache file...')
            os.remove(self.station_cache_data_path)
        print('saving map cache data...')
        with open(self.map_cache_data_path, 'w') as cacheFile:
            json.dump(self.cache, cacheFile)
        print('saving station cache data...')
        with open(self.station_cache_data_path, 'w') as cacheFile:
            json.dump(self.cache_smallest_station, cacheFile)
        with open(self.geocode_cache_data_path, 'w') as cacheFile:
            json.dump(self.cache_geo_code, cacheFile)

    def calculate_distance_and_duration(self, start, dest, mode, departure_hour=9):
        departure_time = datetime(
            self.departure_time.year,
            self.departure_time.month,
            self.departure_time.day,
            departure_hour)
        try:
            self.direction_api_calls = self.direction_api_calls + 1
            if (self.direction_api_calls % 30 == 0):
                print('save cache...')
                self.save_cache()
            print("number of direction api cals: {}".format(self.direction_api_calls))
            directions_result = self.gmaps.directions(
                start, dest, mode, departure_time=departure_time)
        except:
            print('google map direction api error')
            self.save_cache()
            raise
        if (not directions_result) or (not directions_result[0]['legs']):
            return math.inf, math.inf
        else:
            return self.second_to_minute(directions_result[0]['legs'][0]['duration']['value']), \
                self.meter_to_mile(directions_result[0]['legs'][0]['distance']['value'])

    def meter_to_mile(self, meters):
        return meters * 0.000621371

    def second_to_minute(self, seconds):
        return seconds / 60.0

    def calculate_distances_or_durations(
            self,
            start,
            dest,
            mode="walking",
            departure_hour=9,
            distances=True):
        departure_time = datetime(
            self.departure_time.year,
            self.departure_time.month,
            self.departure_time.day,
            departure_hour)
        key = start + dest + mode + str(departure_hour)
        if key in self.cache:
            return self.cache[key]['distance']
        else:
            duration, distance = self.calculate_distance_and_duration(
                start, dest, mode, departure_hour)
            if distances:
                print("adding distance from {} to {}: distance = {} mile"\
                    .format(start, dest, distance))
            else:
                print("adding duration from {} to {}: duration = {} min"\
                    .format(start, dest, duration))
            self.cache[key] = {'distance': distance, 'duration': duration}
            return distance if distances else duration

    def calculate_distances_or_durations_to_dest(
            self,
            start,
            mode="walking",
            departure_hour=9,
            distances=True):
        return self.calculate_distances_or_durations(
                start, self.loc, mode, departure_hour, distances)

    def calculate_distances_or_durations_from_dest(
            self,
            dest,
            mode="walking",
            departure_hour=9,
            distances=True):
        return self.calculate_distances_or_durations(
                self.loc, dest, mode, departure_hour, distances)

    def find_station_with_shortest_time(self, start, stations=None):
        if not stations:
            stations = self.stations
        key = start
        min_duration = math.inf
        best_station = ""
        if key in self.cache_smallest_station:
            return dict([('time_to_shuttle', self.cache_smallest_station[key]['min_duration']), \
                ('best_station', self.cache_smallest_station[key]['best_station'])])
        else:
            print('processing shortest station for {}...'.format(start))
            for station in stations:
                duration, distance = self.calculate_distance_and_duration(
                    start, station, "walking")
                if (duration < min_duration):
                    min_duration = duration
                    best_station = station
            print('adding the shuttle station for {}: min_duration = {}, best_station = {}'\
                .format(start, min_duration, best_station))
            self.cache_smallest_station[key] = {
                'min_duration': min_duration,
                'best_station': best_station,
                }
            return {'time_to_shuttle': min_duration, 'best_station': best_station}

    def get_geocode(self, address):
        if address in self.cache_geo_code :
            return self.cache_geo_code [address]
        else:
            try:
                self.geocode_api_calls = self.geocode_api_calls + 1
                print("number of geocode api cals: {}".format(self.geocode_api_calls))
                geocode = self.gmaps.geocode(address)
                if geocode and 'geometry' in geocode[0] and  'location' in geocode[0]['geometry']:
                    geocode = (geocode[0]['geometry']['location']['lat'], geocode[0]['geometry']['location']['lng'])
                self.cache_geo_code[address] = geocode
                return geocode
            except:
                print('google map api error')
                self.save_cache()
                raise

    def find_approx_station_with_shortest_time(self, start, max_station_considered=3):
        if start in self.cache_smallest_station:
            return {'time_to_shuttle': self.cache_smallest_station[start]['min_duration'], \
                'best_station': self.cache_smallest_station[start]['best_station']}
        start_geocode = self.get_geocode(start)
        print('Calcuating approximate shortest path for {}...'.format(start))
        print(start_geocode)
        print(self.stations_geocode[0])
        stations_considered = []
        for index, station_geocode in enumerate(self.stations_geocode):
            dist = (start_geocode[0] - station_geocode[0])**2 + (start_geocode[1] - station_geocode[1])**2
            if len(stations_considered) < max_station_considered:
                stations_considered.append((dist, self.stations[index]))
            else:
                stations_considered.sort()
                stations_considered[-1] = (dist, self.stations[index])

        stations = [x[1] for x in stations_considered]
        return self.find_station_with_shortest_time(start, stations)
