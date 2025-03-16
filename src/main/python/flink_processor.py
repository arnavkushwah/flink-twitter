import argparse
import logging
import sys
from pyflink.common import WatermarkStrategy, Encoder, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat, FileSink, OutputFileConfig, RollingPolicy

import time

import json
import re
from collections import Counter
import os
from pyflink.common.configuration import Configuration


def word_count(input_path="output-100.jsonl", output_path=None):

    num_parallel = 4

    config = Configuration()
    config.set_integer("taskmanager.numberOfTaskSlots", num_parallel)  

    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(num_parallel)

    # define the source
    if input_path is not None:
        ds = env.from_source(
            source=FileSource.for_record_stream_format(StreamFormat.text_line_format(),
                                                       input_path)
                             .process_static_file_set().build(),
            watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
            source_name="file_source"
        )
    else:
        print("Executing word_count example with default input data set.")
        print("Use --input to specify file input.")

    def split(line):
        yield from line.split()

    def extract_tweet(line):
        try:
            data = json.loads(line)  
            tweet = data.get("text")  
            if tweet:
                yield tweet 
        except json.JSONDecodeError:
            pass  

    def extract_location(line):
        try:
            data = json.loads(line)  
            tweet = data.get("user").get("location")
            if tweet:
                yield tweet  #
        except json.JSONDecodeError:
            pass  

    def extract_tweet_favorites(line):
        try:
            data = json.loads(line)
            tweet = data.get("text")
            favorite_count = int(data.get("favorite_count", 0))  
            user = data.get("user").get("screen_name")
            retweets = int(data.get("retweet_count", 0)) 
            if tweet:
                # print((tweet, favorite_count, user, retweets))
                yield (tweet, favorite_count, user, retweets)
        except (json.JSONDecodeError, ValueError):
            pass  
    
    
    filtered_words = {"the", "is", "of", "at", "this", "who", "who's", "on", "and", "as", "a", "i", "to", "in"}

    # compute the most common keywords
    keyword_ds = ds.flat_map(extract_tweet, output_type=Types.STRING()) \
        .flat_map(lambda line: line.split()) \
        .filter(lambda word: word.lower() not in filtered_words)\
        .map(lambda i: (i, 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
        .key_by(lambda i: i[0]) \
        .reduce(lambda i, j: (i[0], i[1] + j[1]))
    
    # compute the most common locations
    location_ds =  ds.flat_map(extract_location, output_type=Types.STRING()) \
        .map(lambda i: (i, 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
        .key_by(lambda i: i[0]) \
        .reduce(lambda i, j: (i[0], i[1] + j[1]))
    
    # compute the most favorited tweets
    favorite_ds = ds.flat_map(extract_tweet_favorites, output_type=Types.TUPLE([Types.STRING(), Types.INT(), Types.STRING(), Types.INT()])) \
        .key_by(lambda x: x[0]) \
        .reduce(lambda a, b: (a[0], max(a[1], b[1]), a[2] if a[1] >= b[1] else b[2], a[3] if a[1] >= b[1] else b[3]))
    
            
    # output sink (if needed, but should be rerouted to frontend anyways)
    if output_path is not None:
        ds.sink_to(
            sink=FileSink.for_row_format(
                base_path=output_path,
                encoder=Encoder.simple_string_encoder())
            .with_output_file_config(
                OutputFileConfig.builder()
                .with_part_prefix("prefix")
                .with_part_suffix(".ext")
                .build())
            .with_rolling_policy(RollingPolicy.default_rolling_policy())
            .build()
        )
    else:
        print("Printing result to stdout. Use --output to specify output path.")
        
    # submit for execution
    start_time = time.time() 
    env.execute()
    end_time = time.time() 

    # print("Operations Time")
    # print(end_time-start_time)


    keyword_results = list(keyword_ds.execute_and_collect())
    location_results = list(location_ds.execute_and_collect())
    favorite_results = list(favorite_ds.execute_and_collect())

    keyword_results = (sorted(keyword_results,  key=lambda x: x[1], reverse=True)[:6])
    location_results = (sorted(location_results,  key=lambda x: x[1], reverse=True)[:6])
    favorite_results = (sorted(favorite_results,  key=lambda x: x[1], reverse=True)[:6])

    print(keyword_results)

    return keyword_results, location_results, favorite_results



if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        required=False,
        help='Input file to process.')
    parser.add_argument(
        '--output',
        dest='output',
        required=False,
        help='Output file to write results to.')

    argv = sys.argv[1:]
    known_args, _ = parser.parse_known_args(argv)
    
    word_count(known_args.input, known_args.output)
   



