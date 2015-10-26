#!/usr/bin/env python
# coding=utf-8

import argparse
import json
import glob

from scrapy.utils.test import get_crawler
from scrapy.spiders import Spider
from scrapy.statscollectors import MemoryStatsCollector

from spidermon.contrib.scrapy.pipelines import ItemValidationPipeline


def run(args):
    items = load_items(args)
    crawler = get_crawler(Spider, settings_for(args))
    stats = MemoryStatsCollector(crawler)
    crawler.stats = stats
    pipeline = ItemValidationPipeline.from_crawler(crawler)

    print('Validating %d items' % len(items))
    for item in items:
        pipeline.process_item(item, None)

    for key, val in stats.get_stats().iteritems():
        print('%s: %s' % (key, val))


def load_items(args):
    if args.items_file:
        with open(args.items_file) as f:
            return [json.loads(l) for l in f]
    elif args.json_folder:
        jsons = glob.glob('%s/*.json' % args.json_folder)
        items = []

        for json_file in jsons:
            with open(json_file) as f:
                items.append(json.load(f))
        return items


def settings_for(args):
    settings = {}

    if args.jsonschema:
        settings['SPIDERMON_VALIDATION_SCHEMAS'] =  [args.jsonschema]

    return settings


def parse_args():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--items_file', type=str,
                        help='A jsonl file with items.')
    parser.add_argument('--json_folder', type=str,
                        help='Path to a folder full of json files.')
    parser.add_argument('--jsonschema', type=str,
                        help='Json schema to validate the items')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    run(args)
